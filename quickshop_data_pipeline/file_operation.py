import boto3 as bt
import configparser as cp

# reading config.ini
config = cp.ConfigParser()
config.read('config.ini')
aws_access_key_id = config["aws"]["aws_access_key_id"]
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


# function to initialize S3
def initialize_s3():
    s3 = bt.client("s3",
                    aws_access_key_id = aws_access_key_id,
                    aws_secret_access_key = aws_secret_access_key)
    return s3

def read_folder(s3,bucket,folder):
    try:
        file_list = []
        response = s3.list_objects_v2(Bucket=bucket,Prefix=folder)
        for obj in response.get("Contents", []):
            file_list.append(obj["Key"])

    except Exception as e:
        print(f'{folder} not present in bucket {bucket}')
    
    return file_list[1:]

def move_files(s3,bucket_name,src_bucket_name,source_key,destination_key):
    s3.copy_object(
    Bucket=bucket_name,
    CopySource={'Bucket': bucket_name, 'Key': source_key},
    Key=destination_key)
    # Delete the original file
    s3.delete_object(Bucket=bucket_name, Key=source_key)

def delete_files(s3,bucket_name,source_key):
    # Delete the file
    s3.delete_object(Bucket=bucket_name, Key=source_key)

def create_files(s3,bucket,path):
    s3.put_object(Bucket=bucket,Key=path)

def store_data(s3,bucket,path,body):
    s3.put_object(Bucket=bucket,Key=path,Body=body)
