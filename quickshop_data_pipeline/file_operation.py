"""File operation module for S3 and database operations."""
import configparser as cp
import sys
import os
import boto3 as bt

# reading config.ini
config = cp.ConfigParser()
if not os.path.exists('cloud_setup.py'):
    print(f"Error: Configuration file not found.{os.path}")
    sys.exit(1)
config.read('config.ini')
aws_access_key_id = config['aws']["aws_access_key_id"]
aws_secret_access_key = config["aws"]["aws_secret_access_key"]

msg_port = config['email']['smtp_port']
msg_server = config['email']['smtp_server']
msg_sender = config['email']['sender_email']
msg_reciever = config['email']['receiver_email']
msg_password = config['email']['smtp_password']

db_host = config['database']['host']
db_port = config['database']['port']
db_user = config['database']['username']
db_pass = config['database']['password']
db_dbname = config['database']['database_name']


def initialize_s3():
    """Initialize and return an S3 client."""
    s3 = bt.client("s3",
                    aws_access_key_id = aws_access_key_id,
                    aws_secret_access_key = aws_secret_access_key)
    return s3

def read_folder(s3, bucket, folder):
    """Read list of files from S3 folder.

    Args:
        s3: S3 client object.
        bucket: S3 bucket name.
        folder: Folder path in S3.

    Returns:
        List of file keys in the folder.
    """
    try:
        file_list = []
        response = s3.list_objects_v2(Bucket=bucket, Prefix=folder)
        for obj in response.get("Contents", []):
            file_list.append(obj["Key"])
    except FileNotFoundError:
        print(f'{folder} not present in bucket {bucket}')

    return file_list[1:]

def move_files(s3, bucket_name, src_bucket_name, source_key, destination_key):
    """Move file within S3 bucket.

    Args:
        s3: S3 client object.
        bucket_name: Destination bucket name.
        src_bucket_name: Source bucket name.
        source_key: Source file key.
        destination_key: Destination file key.
    """
    s3.copy_object(
        Bucket=bucket_name,
        CopySource={'Bucket': bucket_name, 'Key': source_key},
        Key=destination_key)
    # Delete the original file
    s3.delete_object(Bucket=src_bucket_name, Key=source_key)

def delete_files(s3, bucket_name, source_key):
    """Delete file from S3 bucket.

    Args:
        s3: S3 client object.
        bucket_name: Bucket name.
        source_key: File key to delete.
    """
    s3.delete_object(Bucket=bucket_name, Key=source_key)

def create_files(s3, bucket, path):
    """Create empty file/folder in S3.

    Args:
        s3: S3 client object.
        bucket: Bucket name.
        path: File/folder path.
    """
    s3.put_object(Bucket=bucket, Key=path)

def store_data(s3, bucket, path, body):
    """Store data/file in S3.

    Args:
        s3: S3 client object.
        bucket: Bucket name.
        path: File path in S3.
        body: File content/body to store.
    """
    s3.put_object(Bucket=bucket, Key=path, Body=body)
