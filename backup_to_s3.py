import os
import boto3
from botocore.exceptions import NoCredentialsError
import shutil
from datetime import datetime, timedelta

# AWS Configuration (credentials are fetched from environment variables or AWS CLI config)
S3_BUCKET = "your_s3_bucket_name"
SERVER_NAME = "your_server_name"

# Directories and Retention
LOCAL_DIR = "C:\\path\\to\\your\\config_files"
BACKUP_DIR = "C:\\path\\to\\backup"
LOG_FILE = "C:\\path\\to\\backup\\backup_log.txt"
RETENTION_DAYS = 15  # Files older than this will be deleted

def get_last_run_time():
    """
    Reads the last run time from a log file. If not found, returns a date far in the past.
    """
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as f:
            return datetime.fromisoformat(f.read().strip())
    return datetime.now() - timedelta(days=30)  # Default to 30 days ago

def update_last_run_time():
    """
    Updates the last run time in the log file.
    """
    with open(LOG_FILE, "w") as f:
        f.write(datetime.now().isoformat())

def get_new_files(source_dir, last_run_time):
    """
    Returns a list of files modified after the last run time.
    """
    new_files = []
    for root, _, files in os.walk(source_dir):
        for file in files:
            file_path = os.path.join(root, file)
            if datetime.fromtimestamp(os.path.getmtime(file_path)) > last_run_time:
                new_files.append(file_path)
    return new_files

def create_zip(file_list, zip_file_path):
    """
    Creates a zip file containing the specified files.
    """
    with shutil.ZipFile(zip_file_path, 'w') as zipf:
        for file in file_list:
            arcname = os.path.relpath(file, LOCAL_DIR)
            zipf.write(file, arcname)

def upload_to_s3(file_path, bucket_name, object_name):
    """
    Uploads a file to S3.
    """
    s3_client = boto3.client('s3')  # Credentials are fetched automatically
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"Uploaded: {file_path} to S3://{bucket_name}/{object_name}")
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except NoCredentialsError:
        print("AWS credentials not available.")
    except Exception as e:
        print(f"Error uploading {file_path}: {e}")

def delete_old_files(source_dir, retention_days):
    """
    Deletes files older than the specified retention period.
    """
    cutoff_time = datetime.now() - timedelta(days=retention_days)
    for root, _, files in os.walk(source_dir):
        for file in files:
            file_path = os.path.join(root, file)
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            if file_mod_time < cutoff_time:
                os.remove(file_path)
                print(f"Deleted old file: {file_path}")

def main():
    # Step 1: Check for Last Run Time
    last_run_time = get_last_run_time()
    print(f"Last run time: {last_run_time}")

    # Step 2: Find New/Updated Files
    new_files = get_new_files(LOCAL_DIR, last_run_time)
    if not new_files:
        print("No new or updated files to upload.")
    else:
        # Step 3: Create a Zip File
        if not os.path.exists(BACKUP_DIR):
            os.makedirs(BACKUP_DIR)

        zip_file_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        zip_file_path = os.path.join(BACKUP_DIR, zip_file_name)

        print(f"Zipping {len(new_files)} files to {zip_file_path}")
        create_zip(new_files, zip_file_path)

        # Step 4: Upload to S3
        object_name = f"{SERVER_NAME}/{datetime.now().strftime('%Y-%m-%d')}/{zip_file_name}"
        print(f"Uploading {zip_file_path} to S3...")
        upload_to_s3(zip_file_path, S3_BUCKET, object_name)

        # Step 5: Delete Local Zip File
        print(f"Deleting local zip file: {zip_file_path}")
        os.remove(zip_file_path)

    # Step 6: Delete Config Files Older Than 15 Days
    print(f"Deleting files older than {RETENTION_DAYS} days...")
    delete_old_files(LOCAL_DIR, RETENTION_DAYS)

    # Step 7: Update Last Run Time
    update_last_run_time()

    print("Backup process completed.")

if __name__ == "__main__":
    main()
