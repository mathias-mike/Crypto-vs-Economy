from airflow.configuration import conf as airflow_config
import configparser
import os

config = configparser.ConfigParser()
airflow_dir = os.path.split(airflow_config['core']['dags_folder'])[0]
config.read(f'{airflow_dir}/pipeline.cfg')

S3_BUCKET = config['AWS']['S3_BUCKET']  # Just the <bucket-name>
SCRIPTS_PATH = airflow_dir + '/dags/scripts/'

CLUSTER_NAME = 'crypto_economics'
AWS_REGION = 'us-east-1'
CORE_INSTANCE_COUNT = 2
MASTER_INSTANCE_TYPE = 'c5d.2xlarge'
CORE_INSTANCE_TYPE = 'm5d.2xlarge'
RELEASE_LABEL = 'emr-5.34.0'

CLUSTER_ID = 'cluster_id'
DELETE_CLUSTER = 'delete_cluster'
SUBNET_ID = 'subnet_id'
MASTERE_SG_ID = 'master_sg_id'
SLAVE_SG_ID = 'slave_sg_id'
KEYPAIR_NAME = 'keypair_name'

ECONS_SCRIPT_LAST_RUN = 'econs_script_last_run'

ASSETS_SCRIPT_DONE = 'done_with_assets'
ECONS_SCRIPT_DONE = 'done_with_econs'


JOB_FLOW_ROLE_NAME = 'EMR_EC2_DefaultRole'
SERVICE_ROLE_NAME = 'EMR_DefaultRole'

JOB_FLOW_ROLE_POLICY = """{
    "Version": "2008-10-17", 
    "Statement": [{
        "Sid": "", 
        "Effect": "Allow", 
        "Principal": {"Service": "ec2.amazonaws.com"}, 
        "Action": "sts:AssumeRole" 
    }]
}"""

JOB_FLOW_PERMISSION_POLICY_ARN = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"

SERVICE_ROLE_POLICY = """{
    "Version": "2008-10-17", 
    "Statement": [{
        "Sid": "", 
        "Effect": "Allow", 
        "Principal": {"Service": "elasticmapreduce.amazonaws.com"}, 
        "Action": "sts:AssumeRole"
    }]
}"""

SERVICE_PERMISSION_POLICY_ARN = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"





