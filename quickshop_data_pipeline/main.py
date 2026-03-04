"""Main data pipeline module for processing and validating sales data."""
import datetime as dt
import io
import sys
from dataclasses import dataclass
from typing import List

import pandas as pd
from sqlalchemy import create_engine

import cloud_setup as cs
import email_utility as eu
import file_operation as fo
import file_validation as fv

@dataclass
class PipelineConfig:
    """Container for pipeline configuration and AWS resources."""
    s3: object
    bucket: str
    folders: List[str]
    date_prefix: str

def get_master_data(config: PipelineConfig):
    """Retrieves and formats the master product data."""
    master_files = fo.read_folder(config.s3, config.bucket, config.folders[0])
    if not master_files:
        return None

    obj = config.s3.get_object(Bucket=config.bucket, Key=master_files[0])
    body = obj['Body'].read()
    
    df = pd.read_csv(io.BytesIO(body))
    df = df.loc[:, ['product_id', 'price']]
    return df.astype({'price': 'int32', 'product_id': 'int32'})

def process_single_file(config: PipelineConfig, file_key: str, master_data: pd.DataFrame):
    """Validates a single file and routes it to success or failure destinations."""
    filename = file_key.split('/')[-1].strip()
    obj = config.s3.get_object(Bucket=config.bucket, Key=file_key)
    input_data = pd.read_csv(io.BytesIO(obj['Body'].read()))

    # Data preparation
    input_data = input_data.astype({
        'product_id': 'int32', 'quantity': 'int32',
        'sales': 'int32', 'city': 'string'
    })
    input_data['order_date'] = pd.to_datetime(
        input_data['order_date'], format='%d-%m-%Y', errors='coerce').dt.date

    # Validation
    df_join = input_data.merge(master_data, on='product_id', how='left')
    df_join['Reason'] = df_join.apply(fv.perform_validation, axis=1)
    df_errors = df_join[df_join['Reason'].notna()]

    if not df_errors.empty:
        handle_rejection(config, file_key, filename, df_errors)
        return False
    
    handle_success(config, file_key, filename, df_join)
    return True

def handle_rejection(config: PipelineConfig, file_key: str, filename: str, df_errors: pd.DataFrame):
    """Moves failed files to the rejected folder and saves error logs."""
    move_path = f'{config.folders[2]}{config.date_prefix}/'
    fo.move_files(s3=config.s3, bucket_name=config.bucket, src_bucket_name=config.bucket,
                  source_key=file_key, destination_key=move_path + filename)
    fo.store_data(s3=config.s3, bucket=config.bucket, path=f'{move_path}error_{filename}',
                  body=io.BytesIO(df_errors.to_csv(index=False).encode()))

def handle_success(config: PipelineConfig, file_key: str, filename: str, df_join: pd.DataFrame):
    """Moves successful files and appends data to the database."""
    move_path = f'{config.folders[3]}{config.date_prefix}/'
    fo.move_files(s3=config.s3, bucket_name=config.bucket, src_bucket_name=config.bucket,
                  source_key=file_key, destination_key=move_path + filename)
    try:
        df_orders = df_join[['order_id', 'order_date', 'product_id', 
                             'quantity', 'sales', 'city']]
        db_url = (f'postgresql://{fo.db_user}:{fo.db_pass}'
                  f'@{fo.db_host}:{fo.db_port}/{fo.db_dbname}')
        engine = create_engine(db_url)
        df_orders.to_sql('order_history', engine, if_exists='append', index=False)
    except (OSError, ValueError, RuntimeError) as e:
        print(f'Failed to store data for {filename}. Reason: {e}')

def main():
    """Main execution function for data pipeline."""
    if not cs.create_date_folders():
        print('Folder creation failed. Stopping...')
        sys.exit(1)

    # Encapsulate environment settings into the config object
    config = PipelineConfig(
        s3=fo.initialize_s3(),
        bucket='quickshop-analytics',
        folders=['product_master_data/', 'incoming_files/', 'rejected_files/', 'success_files/'],
        date_prefix=dt.date.today().strftime("%Y%m%d")
    )

    master_data = get_master_data(config)
    if master_data is None:
        print(f'No product file present in {config.folders[0]}')
        sys.exit(0)

    input_path = f'{config.folders[1]}{config.date_prefix}/'
    input_files = fo.read_folder(config.s3, config.bucket, input_path)

    if not input_files:
        print(f'No file present in {input_path}')
        sys.exit(0)

    # Process files and track errors
    error_count = sum(1 for f in input_files if not process_single_file(config, f, master_data))

    # Email reporting
    try:
        msg = eu.setup_mail(total_files=len(input_files), error_files=error_count)
        eu.send_mail(msg=msg)
    except (OSError, ValueError) as e:
        print(f'Email service failed: {e}')

if __name__ == '__main__':
    main()
