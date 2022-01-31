from datetime import datetime, timedelta
from turtle import pu
from airflow import DAG
from airflow.operators.python import PythonOperator
from VariableAvailSensor import VariableAvailSensor
from airflow.models import Variable

import lib.utils as utils
import lib.aws_handler as aws_handler
import lib.spark_handler as spark_handler

import json


def install_dependencies():
    _, emr, _ = aws_handler.get_boto_clients(utils.AWS_REGION, utils.config, emr_get=True)
    cluster_id = Variable.get(utils.CLUSTER_ID)

    step1_name = 'Upgrade numpy'
    args1 = ['python3', '-m', 'pip', 'install', 'numpy', '--upgrade']
    _ = spark_handler.run_cluster_commands(emr, cluster_id, step1_name, args1)


    step2_name = 'Install other required python packages'
    args2 = ['python3', '-m', 'pip', 'install', 'requests', 'pandas', 'pandas-datareader']
    _ = spark_handler.run_cluster_commands(emr, cluster_id, step2_name, args2)



# ----------------CRYPTO------------------------------
def upload_crypto_script_to_s3():
    s3 = aws_handler.get_s3_client(utils.AWS_REGION, utils.config)
    file_name = 'pull_crypto_data.py'
    spark_handler.upload_file_to_s3(s3, utils.S3_BUCKET, utils.S3_SCRIPT_PATH, utils.LOCAL_SCRIPTS_PATH, file_name)


def run_crypto_script(**kwargs):
    _, emr, _ = aws_handler.get_boto_clients(utils.AWS_REGION, utils.config, emr_get=True)
    cluster_id = Variable.get(utils.CLUSTER_ID)
    step_name = "Pull crypto data, transform and load to s3"
    file = utils.S3_SCRIPT_PATH +  'pull_crypto_data.py'
    script_location = 's3://' +  utils.S3_BUCKET + '/' + file

    aws_access_key_id = utils.config['AWS']['ACCESS_KEY_ID']
    aws_secret_access_key = utils.config['AWS']['SECRET_ACCESS_KEY']
    coinapi_api_key = utils.config['COIN_API']['API_KEY']
    start_date = str(datetime.fromisoformat(kwargs['ds']) - timedelta(days=1))
    end_date = str(datetime.fromisoformat(kwargs['ds']))
    symbols = ['COINBASE_SPOT_BTC_USD', 'COINBASE_SPOT_ETH_USD', 'COINBASE_SPOT_LTC_USD']
    period = '1HRS'

    script_args = {}
    script_args['aws_access_key_id'] = aws_access_key_id
    script_args['aws_secret_access_key'] = aws_secret_access_key
    script_args['coinapi_api_key'] = coinapi_api_key
    script_args['start_date'] = start_date
    script_args['end_date'] = end_date
    script_args['symbols'] = symbols
    script_args['period'] = period
    script_args['output_bucket'] = utils.S3_OUTPUT_PATH
    script_args = json.dumps(script_args)


    args = ['spark-submit', script_location, script_args]
    _ = spark_handler.run_cluster_commands(emr, cluster_id, step_name, args)

    s3 = aws_handler.get_s3_client(utils.AWS_REGION, utils.config)
    spark_handler.delete_file_from_s3(s3, utils.S3_BUCKET, file)


    Variable.set(utils.CRYPTO_SCRIPT_DONE, True)



# ---------------STOCK-----------------------------
def upload_stock_scritp_to_s3(**kwargs):
    pull_start_date = datetime.fromisoformat(kwargs['ds']) - timedelta(days=1)
    week_ends = [5, 6]

    if pull_start_date.weekday() in week_ends:
        Variable.set(utils.STOCK_SCRIPT_DONE, True) 
        raise AirflowSkipException(f'No stock data for {pull_start_date.strftime("%a %b %d, %Y")}')

    s3 = aws_handler.get_s3_client(utils.AWS_REGION, utils.config)
    file_name = 'pull_stock_data.py'
    spark_handler.upload_file_to_s3(s3, utils.S3_BUCKET, utils.S3_SCRIPT_PATH, utils.LOCAL_SCRIPTS_PATH, file_name)


def run_stock_script(**kwargs):
    _, emr, _ = aws_handler.get_boto_clients(utils.AWS_REGION, utils.config, emr_get=True)
    cluster_id = Variable.get(utils.CLUSTER_ID)
    step_name = "Pull stock data, transform and load to s3"
    file = utils.S3_SCRIPT_PATH +  'pull_stock_data.py'
    script_location = 's3://' +  utils.S3_BUCKET + '/' + file

    aws_access_key_id = utils.config['AWS']['ACCESS_KEY_ID']
    aws_secret_access_key = utils.config['AWS']['SECRET_ACCESS_KEY']
    _12data_apikey = utils.config['TWELVE_DATA']['API_KEY']
    start_date = str(datetime.fromisoformat(kwargs['ds']) - timedelta(days=1))
    end_date = str(datetime.fromisoformat(kwargs['ds']))
    symbols = 'TSLA,GOOGL,AMZN'
    companies = {"TSLA":"Tesla, Inc.", "GOOGL":"Alphabet Inc.", "AMZN": "Amazon.com, Inc."}
    interval = "1h"

    script_args = {}
    script_args['aws_access_key_id'] = aws_access_key_id
    script_args['aws_secret_access_key'] = aws_secret_access_key
    script_args['_12data_apikey'] = _12data_apikey
    script_args['start_date'] = start_date
    script_args['end_date'] = end_date
    script_args['symbols'] = symbols
    script_args['companies'] = companies
    script_args['interval'] = interval
    script_args['output_bucket'] = utils.S3_OUTPUT_PATH
    script_args = json.dumps(script_args)
    

    args = ['spark-submit', script_location, script_args]
    _ = spark_handler.run_cluster_commands(emr, cluster_id, step_name, args)

    s3 = aws_handler.get_s3_client(utils.AWS_REGION, utils.config)
    spark_handler.delete_file_from_s3(s3, utils.S3_BUCKET, file)

    
    Variable.set(utils.STOCK_SCRIPT_DONE, True) 



# -----------------ECONS--------------------------------------
def upload_econs_script_to_s3(**kwargs):
    econs_last_run = Variable.get(utils.ECONS_SCRIPT_LAST_RUN, default_var=None)
    
    if econs_last_run != None:
        prev_task_run = datetime.fromisoformat(econs_last_run)
        current_dag_run_date = datetime.fromisoformat(kwargs['ds'])

        if prev_task_run < current_dag_run_date - timedelta(days=365):
            Variable.set(utils.ECONS_SCRIPT_DONE, True)
            raise AirflowSkipException(f"No updates yet from last fetch ({prev_task_run}).")
            

    s3 = aws_handler.get_s3_client(utils.AWS_REGION, utils.config)
    file_name = 'pull_econs_data.py'
    spark_handler.upload_file_to_s3(s3, utils.S3_BUCKET, utils.S3_SCRIPT_PATH, utils.LOCAL_SCRIPTS_PATH,  file_name)


def run_econs_script(**kwargs):
    _, emr, _ = aws_handler.get_boto_clients(utils.AWS_REGION, utils.config, emr_get=True)
    cluster_id = Variable.get(utils.CLUSTER_ID)
    step_name = "Pull econs data, transform and load to s3"
    file = utils.S3_SCRIPT_PATH +  'pull_econs_data.py'
    script_location = 's3://' + utils.S3_BUCKET + '/' + file

    aws_access_key_id = utils.config['AWS']['ACCESS_KEY_ID']
    aws_secret_access_key = utils.config['AWS']['SECRET_ACCESS_KEY']
    indicators = [
        {
            "symbol":"SL.UEM.TOTL.NE.ZS",
            "indicator":"Unemployment, total (% of total labor force) (national estimate)"
        },
        {
            "symbol":"NY.GDP.MKTP.CD",
            "indicator":"GDP (current US$)"
        },
        {
            "symbol":"PA.NUS.FCRF",
            "indicator":"Official exchange rate (LCU per US$, period average)"
        },
        {
            "symbol":"FR.INR.RINR",
            "indicator":"Real interest rate (%)"
        }
    ]
    countries = ["US", "NG", "CA", "CN"]
    year = datetime.fromisoformat(kwargs['ds']).year - 2 # As data for current year might not be available in yet
    start_year = year
    end_year = year

    script_args = {}
    script_args['aws_access_key_id'] = aws_access_key_id
    script_args['aws_secret_access_key'] = aws_secret_access_key
    script_args['indicators'] = indicators
    script_args['countries'] = countries
    script_args['start_year'] = start_year
    script_args['end_year'] = end_year
    script_args['output_bucket'] = utils.S3_OUTPUT_PATH
    script_args = json.dumps(script_args)

    args = ['spark-submit', script_location, script_args]
    _ = spark_handler.run_cluster_commands(emr, cluster_id, step_name, args)

    s3 = aws_handler.get_s3_client(utils.AWS_REGION, utils.config)
    spark_handler.delete_file_from_s3(s3, utils.S3_BUCKET, file)


    Variable.set(utils.ECONS_SCRIPT_LAST_RUN, kwargs['ds'])
    Variable.set(utils.ECONS_SCRIPT_DONE, True)



def quality_check():
    s3 = aws_handler.get_s3_client(utils.AWS_REGION, utils.config)
    file_name = 'quality_checks.py'
    spark_handler.upload_file_to_s3(s3, utils.S3_BUCKET, utils.S3_SCRIPT_PATH, utils.LOCAL_SCRIPTS_PATH, file_name)


    _, emr, _ = aws_handler.get_boto_clients(utils.AWS_REGION, utils.config, emr_get=True)
    cluster_id = Variable.get(utils.CLUSTER_ID)
    step_name = "Check quality of data"
    file = utils.S3_SCRIPT_PATH +  file_name
    script_location = 's3://' +  utils.S3_BUCKET + '/' + file

    aws_access_key_id = utils.config['AWS']['ACCESS_KEY_ID']
    aws_secret_access_key = utils.config['AWS']['SECRET_ACCESS_KEY']

    script_args = {}
    script_args['aws_access_key_id'] = aws_access_key_id
    script_args['aws_secret_access_key'] = aws_secret_access_key
    script_args['data_lake_location'] = utils.S3_OUTPUT_PATH
    script_args = json.dumps(script_args)
    

    args = ['spark-submit', script_location, script_args]
    _ = spark_handler.run_cluster_commands(emr, cluster_id, step_name, args)

    s3 = aws_handler.get_s3_client(utils.AWS_REGION, utils.config)
    spark_handler.delete_file_from_s3(s3, utils.S3_BUCKET, file)



def exit_from_dag():
    cluster_id = Variable.get(utils.CLUSTER_ID)

    Variable.delete(utils.CLUSTER_ID)
    Variable.delete(utils.CRYPTO_SCRIPT_DONE)
    Variable.delete(utils.STOCK_SCRIPT_DONE)
    Variable.delete(utils.ECONS_SCRIPT_DONE)

    Variable.set(utils.DELETE_CLUSTER, cluster_id)



default_args = {
    'owner': 'mike',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': datetime(2018, 1, 1),
}


with DAG("spark_dag", 
            default_args=default_args,
            catchup=False,
            description='Pull assets and econs data and save in s3 data lake',
            schedule_interval='@daily') as dag:

    initializing_task = VariableAvailSensor(
        task_id="initializing",
        poke_interval=120,
        varnames=[utils.CLUSTER_ID],
        mode='reschedule'
    )


    install_dependencies_task = PythonOperator(
        task_id="install_dependencies",
        python_callable=install_dependencies
    )


    upload_crypto_script_to_s3_task = PythonOperator(
        task_id="upload_crypto_scritp_to_s3",
        python_callable=upload_crypto_script_to_s3
    )

    run_crypto_script_task = PythonOperator(
        task_id="run_crypto_script",
        python_callable=run_crypto_script,
        provide_context=True
    )


    upload_stock_script_to_s3_task = PythonOperator(
        task_id="upload_stock_scritp_to_s3",
        python_callable=upload_stock_scritp_to_s3
    )

    run_stock_script_task = PythonOperator(
        task_id="run_stock_script",
        python_callable=run_stock_script,
        provide_context=True
    )


    upload_econs_script_to_s3_task = PythonOperator(
        task_id="upload_econs_script_to_s3",
        python_callable=upload_econs_script_to_s3,
        provide_context=True
    )

    run_econs_scripts_task = PythonOperator(
        task_id="run_econs_scripts",
        python_callable=run_econs_script,
        provide_context=True
    )


    wait_for_spark_complete_task = VariableAvailSensor(
        task_id="wait_for_spark_complete",
        poke_interval=120,
        timeout = 600,
        varnames=[utils.ASSETS_SCRIPT_DONE, utils.ECONS_SCRIPT_DONE],
        mode='reschedule'
    )

    quality_check_task = PythonOperator(
        task_id='quality_check',
        python_callable=quality_check
    )

    finish_task = PythonOperator(
        task_id="finish_task",
        python_callable=exit_from_dag
    )


    initializing_task >> install_dependencies_task
    install_dependencies_task >> [upload_crypto_script_to_s3_task, upload_stock_script_to_s3_task, upload_econs_script_to_s3_task]
    upload_crypto_script_to_s3_task >> run_crypto_script_task
    upload_stock_script_to_s3_task >> run_stock_script_task
    upload_econs_script_to_s3_task >> run_econs_scripts_task

    wait_for_spark_complete_task >> quality_check_task
    quality_check_task >> finish_task




