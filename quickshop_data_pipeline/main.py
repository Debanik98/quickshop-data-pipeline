"""Main data pipeline module for processing and validating sales data."""
import datetime as dt
import io
import sys

import pandas as pd
from sqlalchemy import create_engine

import cloud_setup as cs
import email_utility as eu
import file_operation as fo
import file_validation as fv

def get_master_data(s3, bucket, folder):
    """Retrieves and formats the master product data."""
    master_files = fo.read_folder(s3, bucket, folder)
    if not master_files:
        return None

    obj = s3.get_object(Bucket=bucket, Key=master_files[0])
    body = obj['Body'].read()
    
    df = pd.read_csv(io.BytesIO(body))
    df = df.loc[:, ['product_id', 'price']]
    return df.astype({'price': 'int32', 'product_id': 'int32'})

def process_single_file(s3, bucket, file_key, master_data, folders, date_prefix):
    """Validates a single file and routes it to success or failure destinations."""
    filename = file_key.split('/')[-1].strip()
    obj = s3.get_object(Bucket=bucket, Key=file_key)
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
        handle_rejection(s3, bucket, file_key, filename, df_errors, folders[2], date_prefix)
        return False
    
    handle_success(s3, bucket, file_key, filename, df_join, folders[3], date_prefix)
    return True

def handle_rejection(s3, bucket, file_key, filename, df_errors, reject_folder, date_prefix):
    """Moves failed files to the rejected folder and saves error logs."""
    move_path = f'{reject_folder}{date_prefix}/'
    fo.move_files(s3=s3, bucket_name=bucket, src_bucket_name=bucket,
                  source_key=file_key, destination_key=move_path + filename)
    fo.store_data(s3=s3, bucket=bucket, path=f'{move_path}error_{filename}',
                  body=io.BytesIO(df_errors.to_csv(index=False).encode()))

def handle_success(s3, bucket, file_key, filename, df_join, success_folder, date_prefix):
    """Moves successful files and appends data to the database."""
    move_path = f'{success_folder}{date_prefix}/'
    fo.move_files(s3=s3, bucket_name=bucket, src_bucket_name=bucket,
                  source_key=file_key, destination_key=move_path + filename)
    try:
        df_orders = df_join[['order_id', 'order_date', 'product_id', 
                             'quantity', 'sales', 'city']]
        db_url = (f'postgresql://{fo.db_user}:{fo.db_pass}'
                  f'@{fo.db_host}:{fo.db_port}/{fo.db_dbname}')
        engine = create_engine(db_url)
        df_orders.to_sql('order_history', engine, if_exists='append', index=False)
    except (OSError, ValueError) as e:
        print(f'Failed to store data for {filename}. Reason: {e}')

def main():
    """Main execution function for data pipeline."""
    bucket = 'quickshop-analytics'
    folders = ['product_master_data/', 'incoming_files/', 
               'rejected_files/', 'success_files/']

    if not cs.create_date_folders():
        print('Folder creation failed. Stopping...')
        sys.exit(1)

    s3 = fo.initialize_s3()
    master_data = get_master_data(s3, bucket, folders[0])
    
    if master_data is None:
        print(f'No product file present in {folders[0]}')
        sys.exit(0)

    date_prefix = dt.date.today().strftime("%Y%m%d")
    input_path = f'{folders[1]}{date_prefix}/'
    input_files = fo.read_folder(s3, bucket, input_path)

    if not input_files:
        print(f'No file present in {input_path}')
        sys.exit(0)

    error_count = 0
    for file_key in input_files:
        is_success = process_single_file(s3, bucket, file_key, master_data, 
                                         folders, date_prefix)
        if not is_success:
            error_count += 1

    # Email reporting
    try:
        msg = eu.setup_mail(total_files=len(input_files), error_files=error_count)
        eu.send_mail(msg=msg)
    except (OSError, ValueError) as e:
        print(f'Email service failed: {e}')

if __name__ == '__main__':
    main()