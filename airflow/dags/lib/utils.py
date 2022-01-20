from airflow.configuration import conf as airflow_config
import configparser
import os

config = configparser.ConfigParser()
airflow_dir = os.path.split(airflow_config['core']['dags_folder'])[0]
config.read(f'{airflow_dir}/pipeline.cfg')

CLUSTER_NAME = config['AWS']['CLUSTER_NAME']
AWS_REGION = config['AWS']['REGION']

CLUSTER_ID = 'cluster_id'
SUBNET_ID = 'subnet_id'
MASTERE_SG_ID = 'master_sg_id'
SLAVE_SG_ID = 'slave_sg_id'
KEYPAIR_NAME = 'keypair_name'

