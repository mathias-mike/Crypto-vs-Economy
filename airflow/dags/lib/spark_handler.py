import boto3
import logging
import time


def upload_file_to_s3(s3, bucket, s3_path, file_path, file_name):
    file_location = file_path + file_name
    key = s3_path + file_name

    s3.upload_file(file_location, bucket, key)



def get_step_status(emr, cluster_id, step_id):
    response = emr.describe_step(
        ClusterId=cluster_id,
        StepId=step_id
    )

    return response['Step']['Status']['State']     # 'PENDING'|'CANCEL_PENDING'|'RUNNING'|'COMPLETED'|'CANCELLED'|'FAILED'|'INTERRUPTED'



def wait_on_step(emr, cluster_id, step_id):
    required_state = 'COMPLETED'
    current_state = None
    while current_state != required_state:
        time.sleep(5)
        current_state = get_step_status(emr, cluster_id, step_id)

        if current_state in ['CANCELLED','FAILED','INTERRUPTED']:
            raise Exception(f'Step run failed with state: {current_state}')



def run_cluster_commands(emr, cluster_id, step_name, args):
    response = emr.add_job_flow_steps(
                    JobFlowId = cluster_id,
                    Steps=[
                        {
                            'Name': step_name,
                            'ActionOnFailure': 'CONTINUE',
                            'HadoopJarStep': {
                                'Jar': 'command-runner.jar',
                                'Args': args
                            }
                        },
                    ]
                )

    step_id = response['StepIds'][0]

    wait_on_step(emr, cluster_id, step_id)

    return step_id



def delete_file_from_s3(s3, bucket, file):
    response = s3.delete_object(
        Bucket=bucket,
        Key=file
    )

    return response['DeleteMarker']

    











