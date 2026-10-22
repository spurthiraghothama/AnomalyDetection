"""
MinIO Model Store
-----------------
Utility module for uploading/downloading Isolation Forest model checkpoints
to a shared MinIO (S3-compatible) object store. Enables state replication
across scorer nodes so no node cold-starts after a failure.
"""

import os
import io
import tempfile
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "models")
MINIO_USE_SSL = os.getenv("MINIO_USE_SSL", "false").lower() == "true"
MODEL_OBJECT_KEY = "anomaly_model.pkl"


def _get_client():
    """Creates and returns a boto3 S3 client configured for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=f"{'https' if MINIO_USE_SSL else 'http'}://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",  # MinIO requires a region
    )


def ensure_bucket():
    """Creates the model bucket if it doesn't already exist."""
    client = _get_client()
    try:
        client.head_bucket(Bucket=MINIO_BUCKET)
    except ClientError:
        print(f"[ModelStore] Creating bucket: {MINIO_BUCKET}")
        client.create_bucket(Bucket=MINIO_BUCKET)
    return True


def upload_model(local_path, version_tag=None):
    """
    Uploads a model file to MinIO.
    
    Parameters:
    - local_path: Path to the local .pkl model file
    - version_tag: Optional version string (stored as metadata)
    
    Returns:
    - True if upload succeeded
    """
    client = _get_client()
    ensure_bucket()

    metadata = {
        "uploaded-at": datetime.now(timezone.utc).isoformat(),
        "version": version_tag or "latest",
    }

    with open(local_path, "rb") as f:
        client.put_object(
            Bucket=MINIO_BUCKET,
            Key=MODEL_OBJECT_KEY,
            Body=f,
            Metadata=metadata,
        )

    file_size = os.path.getsize(local_path)
    print(f"[ModelStore] Uploaded model to MinIO: {MODEL_OBJECT_KEY} ({file_size} bytes, version={metadata['version']})")
    return True


def download_model(local_path):
    """
    Downloads the latest model checkpoint from MinIO to a local path.
    
    Parameters:
    - local_path: Where to save the downloaded model file
    
    Returns:
    - True if download succeeded, False if no model exists
    """
    client = _get_client()

    try:
        ensure_bucket()
        # Download to a temp file first, then move atomically
        dir_name = os.path.dirname(os.path.abspath(local_path))
        os.makedirs(dir_name, exist_ok=True)

        with tempfile.NamedTemporaryFile(dir=dir_name, delete=False, suffix=".pkl.tmp") as tmp:
            tmp_path = tmp.name

        client.download_file(MINIO_BUCKET, MODEL_OBJECT_KEY, tmp_path)

        # Atomic rename (on same filesystem)
        os.replace(tmp_path, local_path)
        file_size = os.path.getsize(local_path)
        print(f"[ModelStore] Downloaded model from MinIO: {local_path} ({file_size} bytes)")
        return True

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchKey"):
            print("[ModelStore] No model found in MinIO (fresh cluster)")
            return False
        raise
    except Exception as e:
        # Clean up temp file if it exists
        if 'tmp_path' in locals() and os.path.exists(tmp_path):
            os.unlink(tmp_path)
        print(f"[ModelStore] Error downloading model: {e}")
        return False


def get_model_metadata():
    """
    Returns metadata (last-modified timestamp) for the model in MinIO.
    Used by followers to check if a newer version exists.
    
    Returns:
    - dict with 'last_modified' (datetime) and 'version' (str), or None if no model exists
    """
    client = _get_client()
    try:
        ensure_bucket()
        response = client.head_object(Bucket=MINIO_BUCKET, Key=MODEL_OBJECT_KEY)
        return {
            "last_modified": response["LastModified"],
            "content_length": response["ContentLength"],
            "version": response.get("Metadata", {}).get("version", "unknown"),
        }
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchKey"):
            return None
        raise


if __name__ == "__main__":
    print("Testing model_store.py...")
    ensure_bucket()
    
    meta = get_model_metadata()
    if meta:
        print(f"  Model exists: last_modified={meta['last_modified']}, version={meta['version']}")
    else:
        print("  No model in MinIO yet.")
    
    # Test upload/download cycle
    import joblib
    from model.train import train_model
    
    model = train_model(num_samples=100, save_path="/tmp/test_model.pkl")
    upload_model("/tmp/test_model.pkl", version_tag="test-v1")
    download_model("/tmp/test_model_downloaded.pkl")
    
    loaded = joblib.load("/tmp/test_model_downloaded.pkl")
    print(f"  Round-trip test passed: model type = {type(loaded).__name__}")
