from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import logging

import lib.utils as utils
import lib.aws_handler as aws_handler


def setup_cluster_vars():
    Variable.delete(utils.CLUSTER_ID)
    Variable.delete(utils.SUBNET_ID)
    Variable.delete(utils.MASTERE_SG_ID)
    Variable.delete(utils.SLAVE_SG_ID)
    Variable.delete(utils.KEYPAIR_NAME)

    ec2, _, iam = aws_handler.get_boto_clients(utils.AWS_REGION, utils.config, ec2_get=True, iam_get=True)

    vpc_id = aws_handler.get_available_vpc(ec2)

    subnet_id = aws_handler.get_available_subnet(ec2, vpc_id)

    master_sg_id = aws_handler.create_security_group(ec2, vpc_id, 
                                    f'{utils.CLUSTER_NAME}MasterSG', 
                                    f'Master for {utils.CLUSTER_NAME}')

    slave_sg_id = aws_handler.create_security_group(ec2, vpc_id, 
                                    f'{utils.CLUSTER_NAME}SlaveSG',
                                    f'Slave for {utils.CLUSTER_NAME}')

    keypair = aws_handler.get_keypair(ec2, utils.CLUSTER_NAME)

    aws_handler.create_default_roles(
        iam,
        job_flow_role_name=utils.config['EMR']['JOB_FLOW_ROLE_NAME'],
        service_role_name=utils.config['EMR']['SERVICE_ROLE_NAME'],
        job_flow_role_policy=utils.JOB_FLOW_ROLE_POLICY,
        service_role_policy=utils.SERVICE_ROLE_POLICY,
        job_flow_permission_policy_arn=utils.JOB_FLOW_PERMISSION_POLICY_ARN,
        service_permission_policy_arn=utils.SERVICE_PERMISSION_POLICY_ARN
    )

    Variable.set(utils.SUBNET_ID, subnet_id)
    Variable.set(utils.MASTERE_SG_ID, master_sg_id)
    Variable.set(utils.SLAVE_SG_ID, slave_sg_id)
    Variable.set(utils.KEYPAIR_NAME, keypair['KeyName'])

def create_cluster():
    _, emr, _ = aws_handler.get_boto_clients(utils.AWS_REGION, utils.config, emr_get=True)
    cluster_id = aws_handler.create_emr_cluster(
        emr,
        name=utils.CLUSTER_NAME,
        log_uri=utils.config['EMR']['LOG_URI'],
        release_label=utils.config['EMR']['RELEASE_LABEL'],
        master_instance_type=utils.config['EMR']['MASTER_INSTANCE_TYPE'],
        slave_instance_type=utils.config['EMR']['CORE_INSTANCE_TYPE'],
        slave_instance_count=int(utils.config['EMR']['CORE_INSTANCE_COUNT']),
        master_sg_id=Variable.get(utils.MASTERE_SG_ID),
        slave_sg_id=Variable.get(utils.SLAVE_SG_ID),
        keypair_name=Variable.get(utils.KEYPAIR_NAME),
        subnet_id=Variable.get(utils.SUBNET_ID),
        job_flow_role_name=utils.config['EMR']['JOB_FLOW_ROLE_NAME'],
        service_role_name=utils.config['EMR']['SERVICE_ROLE_NAME']
    )
    Variable.set(utils.CLUSTER_ID, cluster_id)

with DAG("cluster_setup_dag", start_date=datetime.now()) as dag:
    setup_cluster_task = PythonOperator(
        task_id="setup_cluster_task",
        python_callable=setup_cluster_vars
    )

    create_cluster_task = PythonOperator(
        task_id="create_cluster_task",
        python_callable=create_cluster
    )

    setup_cluster_task >> create_cluster_task




