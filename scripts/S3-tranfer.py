import boto3 
from boto3.s3.transfer import S3Transfer # type: ignore
import os 

airflow_home = os.environ['AIRFLOW_HOME']

access_key = ''
secret_key = ''

filepath = f'{airflow_home}/data/wikipageviews.gz'

s3_bucket_name = 'myhtd-aws-bucket'

s3_filename = 'wikipageviews.gz'

client = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

tranfer = S3Transfer(client)

tranfer.upload_file(filepath, s3_bucket_name, s3_filename)