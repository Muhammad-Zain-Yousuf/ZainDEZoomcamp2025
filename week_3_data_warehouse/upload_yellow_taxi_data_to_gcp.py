import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import time
import gzip

#Change this to your bucket name
BUCKET_NAME = "week_4_hw4_bucket"  

#If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "zain-de-zoomcamp-2025-36fa41168d02.json"  
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)


BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-"
MONTHS = [f"{i:02d}" for i in range(1, 13)] 
DOWNLOAD_DIR = "."

CHUNK_SIZE = 8 * 1024 * 1024  

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


def download_file(month):
    url = f"{BASE_URL}{month}.csv.gz"
    file_path = os.path.join(DOWNLOAD_DIR, f"fhv_tripdata_2019-{month}.csv.gz")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

def convert_to_csv(file_path):
    try:
        print(f"Opening {file_path}")
        with gzip.open(file_path, 'rt', newline='') as csv_file:
            csv_data = csv_file.read()

            file_path_csv = os.path.join(DOWNLOAD_DIR, file_path[:-3])
            print(f"Converting to CSV file {file_path_csv}")

            with open(file_path_csv, 'wt') as out_file:
                out_file.write(csv_data)
            return file_path_csv

    except Exception as e:
        print(f"Failed to convert {file_path}: {e}")
        return None

def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE  
    
    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")
            
            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")
        
        time.sleep(5)  
    
    print(f"Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths_csv = list(executor.map(convert_to_csv, filter(None, file_paths)))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths_csv))  # Remove None values

    print("All files processed and verified.")