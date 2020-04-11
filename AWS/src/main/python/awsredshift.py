import boto3
import pandas as pd
import json
import configparser as cp
import time

env='AWS'
db='REDSHIFT'
conf = cp.RawConfigParser()
conf.read('../../../resources/application.properties')

aws_key                 = conf.get(env,'AWS_KEY')
aws_secret              = conf.get(env,'AWS_SECRET')
aws_region              = conf.get(env,'REGION')

dwh_cluster_type        = conf.get(db,'DWH_CLUSTER_TYPE')
dwh_node_type           = conf.get(db,'DWH_NODE_TYPE')
dwh_no_of_nodes         = conf.get(db,'DWH_NUM_NODES')
dwh_cluster_identiier   = conf.get(db,'DWH_CLUSTER_IDENTIFIER')
dwh_db_name             = conf.get(db,'DWH_DB_NAME')
dwh_db_user             = conf.get(db,'DWH_DB_MASTER_USER')
dwh_db_password         = conf.get(db,'DWH_DB_MASTER_PASSWORD')
dwh_port                = conf.get(db,'DWH_DB_PORT')

dwh_iam_role_name       = conf.get('IAM','DWH_IAM_ROLE_NAME')
dwh_iam_role_desc       = conf.get('IAM','DWH_IAM_ROLE_DESC')


iam = boto3.client('iam',region_name=aws_region,aws_access_key_id=aws_key,aws_secret_access_key=aws_secret)
redshift = boto3.client('redshift',region_name=aws_region,aws_access_key_id=aws_key,aws_secret_access_key=aws_secret)
ec2 = boto3.resource('ec2',region_name=aws_region,aws_access_key_id=aws_key,aws_secret_access_key=aws_secret)
s3 = boto3.resource('s3',region_name=aws_region,aws_access_key_id=aws_key,aws_secret_access_key=aws_secret)

sampleBucketContent=s3.Bucket("awssampledbuswest2")
for obj in sampleBucketContent.objects.filter(Prefix="ssbgz"):
    print(obj.key)

try:
    # Detach IAM policy
    iam.detach_role_policy(RoleName=dwh_iam_role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
except Exception as e:
    print (e)

try:
    # Delete IAM Role
    iam.delete_role(RoleName=dwh_iam_role_name)
except Exception as e:
    print (e)

try:

    dwhRole = iam.create_role(Path='/',
                          RoleName=dwh_iam_role_name,
                          Description=dwh_iam_role_desc,
                          AssumeRolePolicyDocument=json.dumps(
                              {'Statement': [{'Action': 'sts:AssumeRole',
                                              'Effect': 'Allow',
                                              'Principal': {'Service': 'redshift.amazonaws.com'}}],
                               'Version': '2012-10-17'})

                           )

except Exception as e:
    print (e)

try:
    iam.attach_role_policy (RoleName=dwh_iam_role_name,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']

except Exception as e:
    print (e)


roleArn = iam.get_role(RoleName=dwh_iam_role_name)['Role']['Arn']

print(roleArn)


try:

    response = redshift.create_cluster (
           #Hardware
           ClusterType=dwh_cluster_type,
           NodeType=dwh_node_type,
           NumberOfNodes=int(dwh_no_of_nodes),

           #identifies and creadentials
           DBName=dwh_db_name,
           ClusterIdentifier=dwh_cluster_identiier,
           MasterUsername=dwh_db_user,
           MasterUserPassword=dwh_db_password,

           #roles
           IamRoles=[roleArn]
           )
except Exception as e:
    print (e)



def prettyRedshift(props):
    pd.set_option('display.max_colwidth',-1)
    keysToShow = ['ClusterIdentifier','NodeType','ClusterStatus','MasterUsername','DBName','Endpoint','NumberOfNodes','VpcId']
    x = [(k,v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x,columns=['Key','Value'])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=dwh_cluster_identiier)['Clusters'][0]
df = prettyRedshift(myClusterProps)



for i in range(1,1000):
    print(df)
    if df.loc[df['Key']=='ClusterStatus','Value'].iloc[0]=='available':
        break
    time.sleep(10)
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=dwh_cluster_identiier)['Clusters'][0]
    df = prettyRedshift(myClusterProps)

dwh_endpoint = myClusterProps['Endpoint']['Address']
dwh_roleArn = myClusterProps['IamRoles'][0]['IamRoleArn']
print("DWH_ENDPOINT :: ", dwh_endpoint)
print("DWH_ROLE_ARN :: ", dwh_roleArn)

try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(dwh_port),
        ToPort=int(dwh_port)
    )
except Exception as e:
    print(e)

redshift.delete_cluster( ClusterIdentifier=dwh_cluster_identiier,  SkipFinalClusterSnapshot=True)

myClusterProps = redshift.describe_clusters(ClusterIdentifier=dwh_cluster_identiier)['Clusters'][0]
df = prettyRedshift (myClusterProps)
for i in range(1,1000):
    try:
        print(df)
        if df.loc[df['Key'] == 'ClusterStatus', 'Value'].iloc[0] == 'deleting':
            time.sleep(10)
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=dwh_cluster_identiier)['Clusters'][0]
        df = prettyRedshift(myClusterProps)
    except Exception as e:
        print("Cluster deleted")
        break

try:
    # Detach IAM policy
    iam.detach_role_policy(RoleName=dwh_iam_role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
except Exception as e:
    print (e)

try:
    # Delete IAM Role
    iam.delete_role(RoleName=dwh_iam_role_name)
except Exception as e:
    print (e)