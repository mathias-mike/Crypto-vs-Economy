from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from VariableAvailSensor import VariableAvailSensor
from airflow.models import Variable

import lib.utils as utils
import lib.aws_handler as aws_handler
import lib.spark_handler as spark_handler


def upload_assets_scritp_to_s3():
    s3 = aws_handler.get_s3_client(utils.AWS_REGION, utils.config)
    s3_path = 'crypto_vs_econs/scripts/'
    file_name = 'pull_assets_data.py'
    file_path = './scripts/'

    spark_handler.upload_file_to_s3(s3, utils.S3_BUCKET, s3_path, file_path, file_name)



def upload_econs_script_to_s3(**kwargs):
    prev_task_run = Variable.get(utils.ECONS_SCRIPT_LAST_RUN, default_var=None)
    current_dag_run_date = kwargs['ds']
    
    if prev_task_run != None:
        if prev_task_run < current_dag_run_date - timedelta(days=365):

            Variable.set(utils.ECONS_SCRIPT_DONE, True)

            raise AirflowSkipException(f"No updates yet from last fetch ({prev_task_run}).")

    s3 = aws_handler.get_s3_client(utils.AWS_REGION, utils.config)
    s3_path = 'crypto_vs_econs/scripts/'
    file_name = 'pull_econs_data.py'
    file_path = './scripts/'

    spark_handler.upload_file_to_s3(s3, utils.S3_BUCKET, s3_path, file_path, file_name)

    Variable.set(utils.ECONS_SCRIPT_LAST_RUN, current_dag_run_date)



def run_assets_script(**kwargs):
    _, emr, _ = aws_handler.get_boto_clients(utils.AWS_REGION, utils.config, emr_get=True)
    cluster_id = Variable.get(utils.CLUSTER_ID)
    step_name = "Pull assets data, transform and load to s3"
    script_location = utils.S3_BUCKET + 'crypto_vs_econs/scripts/' +  'pull_assets_data.py'

    aws_access_key_id = utils.config['AWS']['ACCESS_KEY_ID']
    aws_secret_access_key = utils.config['AWS']['SECRET_ACCESS_KEY']
    _12data_apikey = utils.config['TWELVE_DATA']['API_KEY']
    start_date = str(kwargs['ds'] - timedelta(days=1))
    end_date = str(kwargs['ds'])
    symbols = 'AAPL,TSLA,GOOGL,AMZN,BTC/USD,ETH/USD,BNB/USD,LTC/USD'
    companies = '''{
        "AAPL":"Apple Inc.",
        "TSLA":"Tesla, Inc.",
        "GOOGL":"Alphabet Inc.",
        "AMZN": "Amazon.com, Inc."
    }'''
    interval = "1h"
    output_bucket = utils.S3_BUCKET # Just the bucket s3 url

    script_args = f"""{{
        "aws_access_key_id": "{aws_access_key_id}",
        "aws_secret_access_key": "{aws_secret_access_key}",
        "_12data_apikey": "{_12data_apikey}",
        "start_date": "{start_date}",
        "end_date": "{end_date}",
        "symbols": "{symbols}",
        "companies": {companies},
        "interval": "{interval}",
        "output_bucket": "{output_bucket}"
    }}"""

    spark_handler.submit_spark_job(emr, cluster_id, step_name, script_location, script_args)

    Variable.set(utils.ASSETS_SCRIPT_DONE, True)



def run_econs_script(**kwargs):
    _, emr, _ = aws_handler.get_boto_clients(utils.AWS_REGION, utils.config, emr_get=True)
    cluster_id = Variable.get(utils.CLUSTER_ID)
    step_name = "Pull econs data, transform and load to s3"
    script_location = utils.S3_BUCKET + 'crypto_vs_econs/scripts/' +  'pull_econs_data.py'

    aws_access_key_id = utils.config['AWS']['ACCESS_KEY_ID']
    aws_secret_access_key = utils.config['AWS']['SECRET_ACCESS_KEY']
    indicators = '''[
        {
            "symbol":"SL.UEM.TOTL.NE.ZS",
            "indicator":"Unemployment, total (% \of total labor force) (national estimate)"
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
        },
        {
            "symbol":"SP.POP.TOTL",
            "indicator":"Population, total"
        },
    ]'''
    countries = '["US", "NG", "UK", "CA", "CN"]'

    year = kwargs['ds'].year - 1 # As data for current year might not be filled in yet
    start_year = year
    end_year = year
    output_bucket = utils.S3_BUCKET # Just the bucket s3 url

    script_args = f"""{{
        "aws_access_key_id": "{aws_access_key_id}",
        "aws_secret_access_key": "{aws_secret_access_key}",
        "indicators": "{indicators}",
        "countries": "{countries}",
        "start_year": {start_year},
        "end_year": {end_year},
        "output_bucket": "{output_bucket}"
    }}"""

    spark_handler.submit_spark_job(emr, cluster_id, step_name, script_location, script_args)
    Variable.set(utils.ECONS_SCRIPT_DONE, True)


def exit_from_dag():
    cluster_id = Variable.get(utils.CLUSTER_ID)

    Variable.delete(utils.CLUSTER_ID)
    Variable.delete(utils.ECONS_SCRIPT_DONE)
    Variable.delete(utils.ECONS_SCRIPT_DONE)

    Variable.set(utils.DELETE_CLUSTER, cluster_id)



with DAG("cluster_setup_dag", start_date=datetime.now()) as dag:

    initializing_task = VariableAvailSensor(
        task_id="initializing",
        poke_interval=120,
        varname=[utils.CLUSTER_ID],
        mode='reschedule'
    )


    upload_assets_scritp_to_s3_task = PythonOperator(
        task_id="upload_assets_scritp_to_s3",
        python_callable=upload_assets_scritp_to_s3
    )


    upload_econs_script_to_s3_task = PythonOperator(
        task_id="upload_econs_script_to_s3",
        python_callable=upload_econs_script_to_s3,
        provide_context=True
    )


    run_assets_script_task = PythonOperator(
        task_id="run_assets_script_task",
        python_callable=run_assets_script,
        provide_context=True
    )


    run_econs_scripts_task = PythonOperator(
        task_id="run_econs_scripts_task",
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

    finish_task = PythonOperator(
        task_id="finish_task",
        python_callable=exit_from_dag
    )


    initializing_task >> [upload_assets_scritp_to_s3_task, upload_econs_script_to_s3_task]
    upload_assets_scritp_to_s3_task >> run_assets_script_task
    upload_econs_script_to_s3_task >> run_econs_scripts_task

    wait_for_spark_complete_task >> finish_task




