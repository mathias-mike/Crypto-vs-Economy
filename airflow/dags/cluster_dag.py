from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from VariableAvailSensor import VariableAvailSensor
from airflow.models import Variable

import lib.utils as utils
import lib.aws_handler as aws_handler


def setup_cluster_vars():
    Variable.delete(utils.CLUSTER_ID)
    Variable.delete(utils.SUBNET_ID)
    Variable.delete(utils.MASTERE_SG_ID)
    Variable.delete(utils.SLAVE_SG_ID)
    Variable.delete(utils.KEYPAIR_NAME)
    Variable.delete(utils.DELETE_CLUSTER)
    Variable.delete(utils.ASSETS_SCRIPT_DONE)
    Variable.delete(utils.ECONS_SCRIPT_DONE)

    ec2, _, iam = aws_handler.get_boto_clients(utils.AWS_REGION, utils.config, ec2_get=True, iam_get=True)

    vpc_id = aws_handler.get_available_vpc(ec2)

    subnet_id = aws_handler.get_available_subnet(ec2, vpc_id)

    master_sg_id = aws_handler.create_security_group(ec2, vpc_id, 
                                    f'{utils.CLUSTER_NAME}MasterSG', 
                                    f'Master for {utils.CLUSTER_NAME}')

    slave_sg_id = aws_handler.create_security_group(ec2, vpc_id, 
                                    f'{utils.CLUSTER_NAME}SlaveSG',
                                    f'Slave for {utils.CLUSTER_NAME}')

    keypair_name = aws_handler.get_keypair(ec2, utils.CLUSTER_NAME)

    aws_handler.create_default_roles(
        iam,
        job_flow_role_name=utils.JOB_FLOW_ROLE_NAME,
        service_role_name=utils.SERVICE_ROLE_NAME,
        job_flow_role_policy=utils.JOB_FLOW_ROLE_POLICY,
        service_role_policy=utils.SERVICE_ROLE_POLICY,
        job_flow_permission_policy_arn=utils.JOB_FLOW_PERMISSION_POLICY_ARN,
        service_permission_policy_arn=utils.SERVICE_PERMISSION_POLICY_ARN
    )

    Variable.set(utils.SUBNET_ID, subnet_id)
    Variable.set(utils.MASTERE_SG_ID, master_sg_id)
    Variable.set(utils.SLAVE_SG_ID, slave_sg_id)
    Variable.set(utils.KEYPAIR_NAME, keypair_name)


def create_cluster():
    _, emr, _ = aws_handler.get_boto_clients(utils.AWS_REGION, utils.config, emr_get=True)
    
    cluster_id = aws_handler.create_emr_cluster(
        emr,
        name=utils.CLUSTER_NAME,
        log_uri=utils.CLUSTER_LOG_URI,
        release_label=utils.RELEASE_LABEL,
        master_instance_type=utils.MASTER_INSTANCE_TYPE,
        slave_instance_type=utils.CORE_INSTANCE_TYPE,
        slave_instance_count=utils.CORE_INSTANCE_COUNT,
        master_sg_id=Variable.get(utils.MASTERE_SG_ID),
        slave_sg_id=Variable.get(utils.SLAVE_SG_ID),
        keypair_name=Variable.get(utils.KEYPAIR_NAME),
        subnet_id=Variable.get(utils.SUBNET_ID),
        job_flow_role_name=utils.JOB_FLOW_ROLE_NAME,
        service_role_name=utils.SERVICE_ROLE_NAME
    )
    Variable.set(utils.CLUSTER_ID, cluster_id)


def terminate_cluster():
    _, emr, _ = aws_handler.get_boto_clients(utils.AWS_REGION, utils.config, emr_get=True)

    aws_handler.terminate_cluster(emr, Variable.get(utils.DELETE_CLUSTER))



def del_keypair_and_security_group():
    ec2, _, _ = aws_handler.get_boto_clients(utils.AWS_REGION, utils.config, ec2_get=True)

    aws_handler.del_keypair(ec2, utils.CLUSTER_NAME)

    aws_handler.del_security_groups(ec2, Variable.get(utils.MASTERE_SG_ID), Variable.get(utils.SLAVE_SG_ID))


def del_roles():
    _, _, iam = aws_handler.get_boto_clients(utils.AWS_REGION, utils.config, iam_get=True)

    aws_handler.del_roles(iam, 
        utils.JOB_FLOW_ROLE_NAME, 
        utils.SERVICE_ROLE_NAME, 
        utils.JOB_FLOW_PERMISSION_POLICY_ARN, 
        utils.SERVICE_PERMISSION_POLICY_ARN
    )


default_args = {
    'owner': 'mike',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': datetime(2018, 1, 1),
}


with DAG("cluster_dag",
            default_args=default_args,
            catchup=False,
            description='Setup and run AWS EMR cluster for spark Job',
            schedule_interval='@daily') as dag:

    setup_cluster_task = PythonOperator(
        task_id="setup_cluster_task",
        python_callable=setup_cluster_vars
    )

    create_cluster_task = PythonOperator(
        task_id="create_cluster_task",
        python_callable=create_cluster
    )

    wait_for_spark_runs_task = VariableAvailSensor(
        task_id="wait_for_spark_runs",
        poke_interval=120,
        varnames=[utils.DELETE_CLUSTER],
        mode='reschedule'
    )

    terminate_cluster_task = PythonOperator(
        task_id="terminate_cluster_task",
        python_callable=terminate_cluster
    )

    del_keypair_and_security_group_task = PythonOperator(
        task_id="del_keypair_and_security_group_task",
        python_callable=del_keypair_and_security_group
    )

    del_roles_task = PythonOperator(
        task_id="del_roles_task",
        python_callable=del_roles
    )



    setup_cluster_task >> create_cluster_task
    create_cluster_task >> wait_for_spark_runs_task
    wait_for_spark_runs_task >> terminate_cluster_task
    terminate_cluster_task >> del_keypair_and_security_group_task
    terminate_cluster_task >> del_roles_task







