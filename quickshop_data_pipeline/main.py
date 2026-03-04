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

def main():
    """Main execution function for data pipeline.

    Processes incoming sales data files, validates them against master data,
    handles errors, and stores valid data in the database.
    """
    bucket = 'quickshop-analytics'
    folders = ['product_master_data/','incoming_files/','rejected_files/','success_files/']

    creation_flag = cs.create_date_folders()

    if not creation_flag:
        print('folder creation failed. Stopping process...')
        sys.exit(1)

    s3 = fo.initialize_s3()

    # read the master data
    master_files = fo.read_folder(s3, bucket, folders[0])

    if not master_files:
        print(f'No product file present in {folders[0]}')
        sys.exit(0)

    key = master_files[0]

    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj['Body'].read()  # read once

    # parse CSV from bytes
    master_data = pd.read_csv(io.BytesIO(body))
    master_data = master_data.loc[:,['product_id','price']]
    master_data = master_data.astype({'price': 'int32', 'product_id': 'int32'})
    print(master_data.head())

    date_prefix = dt.date.today().strftime("%Y%m%d")
    full_path = f'{folders[1]}{date_prefix}/'


    # read the incoming data
    input_files = fo.read_folder(s3, bucket, full_path)

    if len(input_files) == 0:
        print(f'No file present in {folders[1]}')
        sys.exit(0)
    else:
        print(input_files)

    total_files = 0
    error_files = 0

    for i in input_files[:]:

        filename = i.split('/')[-1].strip()
        obj = s3.get_object(Bucket=bucket, Key=i)
        body = obj['Body'].read()  # read once
        # parse CSV from bytes
        input_data = pd.read_csv(io.BytesIO(body))

        # data preparation
        input_data = input_data.astype({'product_id':'int32'
                                        ,'quantity':'int32'
                                        ,'sales':'int32'
                                        ,'city':'string'})
        
        input_data['order_date'] = pd.to_datetime(
            input_data['order_date'], format='%d-%m-%Y', errors='coerce').dt.date

        df_join = input_data.merge(master_data, on='product_id', how='left')

        # Validation
        df_join['Reason'] = df_join.apply(fv.perform_validation, axis=1)

        print(df_join.head())

        df_error_row = df_join[df_join['Reason'].notna()]

        print(df_error_row.head())

        total_files += 1

        # File movement
        if not df_error_row.empty:
            error_files += 1
            # Move file to rejected folder
            move_path = f'{folders[2]}{date_prefix}/'
            fo.move_files(s3=s3,
                          bucket_name=bucket,
                          src_bucket_name=bucket,
                          source_key=full_path + filename,
                          destination_key=move_path + filename)

            fo.store_data(
                s3=s3,
                bucket=bucket,
                path=f'{move_path}error_{filename}',
                body=io.BytesIO(df_error_row.to_csv(index=False).encode())
            )
        else:
            # Move file to success folder
            move_path = f'{folders[3]}{date_prefix}/'
            fo.move_files(s3=s3,
                          bucket_name=bucket,
                          src_bucket_name=bucket,
                          source_key=full_path + filename,
                          destination_key=move_path + filename)

            # Store the data in DB
            try:
                df_orders = df_join[['order_id', 'order_date', 'product_id',
                                      'quantity', 'sales', 'city']]
                path = (f'postgresql://{fo.db_user}:{fo.db_pass}'
                        f'@{fo.db_host}:{fo.db_port}/{fo.db_dbname}')
                engine = create_engine(path)
                df_orders.to_sql('order_history', engine, if_exists='append',
                                 index=False)
                print('Data inserted to table successfully')
            except (OSError, ValueError) as e:
                print(f'Failed to store data. Reason: {e}')

    # Send confirmation mail
    try:
        msg = eu.setup_mail(total_files=total_files, error_files=error_files)
        eu.send_mail(msg=msg)
        print('Confirmation email sent')
    except (OSError, ValueError) as e:
        print(f'Email service failed. Reason: {e}')


if __name__ == '__main__':
    main()

