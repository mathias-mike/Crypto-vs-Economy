import boto3
from botocore.exceptions import ClientError
import logging

def get_boto_clients(region_name, config=None, ec2_get=False, emr_get=False, iam_get=False):
    ec2 = None
    emr = None
    iam = None

    if config != None and config['AWS']['ACCESS_KEY_ID'] != '' and config['AWS']['SECRET_ACCESS_KEY'] != '':
        if ec2_get: ec2 = boto3.client("ec2", region_name=region_name, 
                                aws_access_key_id=config['AWS']['ACCESS_KEY_ID'], 
                                aws_secret_access_key=config['AWS']['SECRET_ACCESS_KEY'])
        
        if emr_get: emr = boto3.client("emr", region_name=region_name, 
                                aws_access_key_id=config['AWS']['ACCESS_KEY_ID'], 
                                aws_secret_access_key=config['AWS']['SECRET_ACCESS_KEY'])
        
        if iam_get: iam = boto3.client("iam", region_name=region_name, 
                                aws_access_key_id=config['AWS']['ACCESS_KEY_ID'], 
                                aws_secret_access_key=config['AWS']['SECRET_ACCESS_KEY'])
        
    else:
        if ec2_get: ec2 = boto3.client("ec2", region_name=region_name)
        if emr_get: emr = boto3.client("emr", region_name=region_name)
        if iam_get: iam = boto3.client("iam", region_name=region_name)

    return ec2, emr, iam


def get_available_vpc(ec2):
    return ec2.describe_vpcs(Filters=[{'Name': 'state', 'Values': ['available']}]) \
                .get('Vpcs', [{}])[0] \
                .get('VpcId', None)


def get_available_subnet(ec2, vpc_id):
    return ec2.describe_subnets(
                    Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}, 
                            {'Name': 'state', 'Values': ['available']}]
                ) \
                .get('Subnets', [{}])[0] \
                .get('SubnetId', None)


def get_keypair(ec2, cluster_name):
    keypairs = ec2.describe_key_pairs(Filters=[{'Name': 'key-name', 'Values': [cluster_name]}]) \
                    .get('KeyPairs', [{}])
    
    if len(keypairs) == 0:
        keypair = ec2.create_key_pair(KeyName=cluster_name)
    else:
        keypair = keypairs[0]

    return keypair


def create_default_roles(iam, job_flow_role_name, service_role_name):
    try:
        job_flow_role = iam.get_role(RoleName=job_flow_role_name)
        service_role = iam.get_role(RoleName=service_role_name)
        instance_profile = iam.get_instance_profile(InstanceProfileName=job_flow_role_name)
    except iam.exceptions.NoSuchEntityException as e:
        try:
            job_flow_role = iam.create_role(
                RoleName=job_flow_role_name,
                Path='/',
                AssumeRolePolicyDocument='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role'
            )

            service_role = iam.create_role(
                RoleName=service_role_name,
                Path='/',
                AssumeRolePolicyDocument='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
            )

            instance_profile = iam.create_instance_profile(
                InstanceProfileName=job_flow_role_name,
                Path='/'
            )
        except Exception as e:
            logging.error(f'Exception output @get_default_roles:\n{e}')



def create_security_group(ec2, vpc_id, group_name, group_description):
    group_id = None
    try:
        groups = ec2.describe_security_groups(
            Filters=[
                {'Name': 'group-name', 'Values': [group_name]},
                {'Name': 'vpc-id', 'Values': [vpc_id]}]
        ).get('SecurityGroups', [{}])

        if len(groups) == 0:
            group_id = ec2.create_security_group(
                GroupName=group_name,
                VpcId=vpc_id,
                Description=group_description
            )['GroupId']
        else:
            group_id = groups[0]['GroupId']
    
    except ClientError as e:
        logging.error(f'Exception output @create_security_group:\n{e}')

    return group_id


def create_emr_cluster(emr, name, 
        log_uri=None,
        release_label='emr-5.34.0',
        master_instance_type=None,
        slave_instance_type=None,
        slave_instance_count=3,
        master_sg_id=None,
        slave_sg_id=None,
        keypair_name=None,
        subnet_id=None,
        job_flow_role_name='EMR_EC2_DefaultRole',
        service_role_name='EMR_DefaultRole' ):

    clusters = emr.list_clusters(ClusterStates=['STARTING', 'RUNNING', 'WAITING', 'BOOTSTRAPPING'])
    active_clusters = [cluster for cluster in clusters['Clusters'] if cluster['Name']==name]

    if len(active_clusters) == 0:
        cluster_response = emr.run_job_flow(
            Name=name,
            LogUri=log_uri,
            ReleaseLabel=release_label,
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master Nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': master_instance_type,
                        'InstanceCount': 1
                    },
                    {
                        'Name': 'Slave Nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': slave_instance_type,
                        'InstanceCount': slave_instance_count
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': True,
                'Ec2KeyName': keypair_name,
                'Ec2SubnetId': subnet_id,
                'EmrManagedMasterSecurityGroup': master_sg_id,
                'EmrManagedSlaveSecurityGroup': slave_sg_id
            },
            VisibleToAllUsers=True,
            JobFlowRole=job_flow_role_name,
            ServiceRole=service_role_name,
            Applications=[
                {'Name': 'hadoop'},
                {'Name': 'spark'},
                {'Name': 'hive'},
                {'Name': 'zeppelin'}
            ]
        )
        return cluster_response['JobFlowId']

    else:
        return active_clusters[0]['Id']


