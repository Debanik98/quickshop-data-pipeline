import file_operation as fo
import datetime as dt
import os

def create_date_folders():
    s3 = fo.initialize_s3()

    bucket = 'namastekart-analytics'
    folders = ['product_master_data/','incoming_files/','rejected_files/','success_files/']
    date_folder = dt.date.today().strftime(format='%Y%m%d')

    try:
        for i in folders[1:]:
            path = f'{i}{date_folder}/'
            fo.create_files(s3=s3,bucket=bucket,path=path)
            print(f'folder creation completed at {path}')
    except Exception as e:
        print(f'folder creation failed. Reason: {e}')
        return False

    #Place files in S3 incoming folder
    source_path = f'C:\\Users\\deban\\OneDrive\\Desktop\\file'
    base_dest_path = f'{folders[1]}{date_folder}/'

    for root, dirs, files in os.walk(source_path):
        for file in files:
            local_path = os.path.join(root, file)
            dest_path = base_dest_path+file
            try:
                s3.upload_file(local_path, bucket, dest_path)
                print(f"Upload Successful: {dest_path}")
            except FileNotFoundError:
                print("The file was not found")
                return False
            except Exception as e:
                print(f"upload failure: {e}")
                return False

    return True

    