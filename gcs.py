import os
from google.cloud import storage

BUCKET = "gocod"
path_to_local_home = "./stockage"
client = storage.Client()
bucket = client.bucket(BUCKET)

for filename in os.listdir(path_to_local_home):
        if filename.endswith(".txt"):  # Only upload .txt files
            local_file_path = os.path.join(path_to_local_home, filename)
            
            # Create a blob object in the bucket
            blob = bucket.blob(filename)
            
            # Upload the file to GCS
            blob.upload_from_filename(local_file_path)
            
            print(f"File {filename} uploaded to GCS bucket {BUCKET}.")
