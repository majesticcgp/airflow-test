import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def local_to_s3(
    bucket_name: str, 
    key: str, 
    file_name: str, 
    remove_local: bool = False,
) -> None:
    s3 = S3Hook(aws_conn_id='aws-conn-id')
    s3.load_file(
        filename=file_name, 
        bucket_name=bucket_name, 
        replace=True, 
        key=key
    )
    if remove_local:
        if os.path.isfile(file_name):
            os.remove(file_name)
            
