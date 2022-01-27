import boto3
import logging
import time


def upload_file_to_s3(s3, bucket, s3_path, file_path, file_name):
    s3_location = bucket + s3_path
    file_location = file_path + file_name

    s3.upload_file(file_location, s3_location, file_name)


def submit_spark_job(emr, cluster_id, step_name, script_location, script_args):
    response = emr.add_job_flow_steps(
                    JobFlowId = cluster_id,
                    Steps=[
                        {
                            'Name': step_name,
                            'ActionOnFailure': 'CONTINUE',
                            'HadoopJarStep': {
                                'Jar': 'command-runner.jar',
                                'Args': [
                                    'spark-submit',
                                    '–deploy-mode',
                                    'cluster',
                                    '–master',
                                    'yarn',
                                    '–conf',
                                    'spark.yarn.submit.waitAppCompletion=true',
                                    script_location,
                                    script_args
                                ]
                            }
                        },
                    ]
                )

    return response['StepIds'][0]


def get_step_status(emr, cluster_id, step_id):
    response = emr.describe_step(
        ClusterId=cluster_id,
        StepId=step_id
    )

    return response['Step']['Status']['State']


def wait_on_step(emr, cluster_id, step_id):
    required_state = 'COMPLETED'
    current_state = None
    while current_state != required_state:
        time.sleep(5)
        current_state = get_step_status(emr, cluster_id, step_id)

        if current_state in ['CANCELLED','FAILED','INTERRUPTED']:
            raise Exception(f'Step run failed with state {current_state}')



def delete_file_from_s3(s3, bucket, file):
    location = bucket + file

    











