import os
from pathlib import Path
from minio import Minio
from dotenv import load_dotenv

def upload_directory_to_minio(local_path: Path, bucket_name: str, minio_client):
    for file_path in local_path.rglob("*.json"):

        relative_path = file_path.relative_to(Path("/opt/airflow/data/raw"))
        object_name = str(relative_path).replace("\\", "/")

        print(f"Uploading {file_path} to bucket '{bucket_name}' as '{object_name}'")

        minio_client.fput_object(
            bucket_name,
            object_name,
            str(file_path)
        )

def main():

    load_dotenv()

    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_access = os.getenv("MINIO_ACCESS_KEY")
    minio_secret = os.getenv("MINIO_SECRET_KEY")

    client = Minio(
        minio_endpoint,
        access_key = minio_access,
        secret_key = minio_secret,
        secure = False
    )

    bucket = "raw"

    local_weather_path = Path("/opt/airflow/data/raw/api/weather")

    if not local_weather_path.exists():
        raise FileNotFoundError(f"{local_weather_path} not found. Please run fetch_weather_api.py first.")
    
    upload_directory_to_minio(local_weather_path, bucket, client)

    print("Upload completed successfully.")

if __name__ == "__main__":
    main()