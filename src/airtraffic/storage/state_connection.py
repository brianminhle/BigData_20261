"""MinIO configuration shared by state ingestion and local batch jobs."""

import os
from airtraffic.storage.snapshots import SnapshotStore


def connect_store(endpoint=None, bucket=None, insecure=False):
    from minio import Minio
    from urllib3 import PoolManager, Timeout

    endpoint = endpoint or os.getenv('AIRTRAFFIC_MINIO_ENDPOINT')
    bucket = bucket or os.getenv('AIRTRAFFIC_STATES_BUCKET', 'airtraffic-states')
    access = os.getenv('AIRTRAFFIC_MINIO_ACCESS_KEY')
    secret = os.getenv('AIRTRAFFIC_MINIO_SECRET_KEY')
    if not endpoint or not access or not secret:
        raise ValueError('Set MinIO endpoint, AIRTRAFFIC_MINIO_ACCESS_KEY and AIRTRAFFIC_MINIO_SECRET_KEY')
    client = Minio(endpoint, access_key=access, secret_key=secret, secure=not insecure,
                   http_client=PoolManager(timeout=Timeout(connect=10, read=60), retries=2))
    return SnapshotStore(client, bucket)
