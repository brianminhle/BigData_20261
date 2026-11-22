"""Content-addressed raw objects with a manifest as the commit marker."""

from dataclasses import asdict
from datetime import datetime, timezone
import io
import json
from pathlib import Path

from minio.error import S3Error

from airtraffic.ingestion.catalog import SourceFile


class RawObjectStore:
    def __init__(self, client, bucket: str):
        self.client = client
        self.bucket = bucket

    def ensure_bucket(self):
        if not self.client.bucket_exists(self.bucket):
            raise ValueError(f"Create the destination bucket first: {self.bucket}")

    def _stat(self, key):
        try:
            return self.client.stat_object(self.bucket, key)
        except S3Error as error:
            if error.code in {"NoSuchKey", "NoSuchObject"}:
                return None
            raise

    @staticmethod
    def manifest_key(source: SourceFile) -> str:
        return f"manifests/{source.dataset}/{source.identity}.json"

    def is_committed(self, source: SourceFile) -> bool:
        return self._stat(self.manifest_key(source)) is not None

    def commit(self, source: SourceFile, path: Path, checksum: str, statistics: dict):
        key = f"raw/{source.dataset}/sha256/{checksum}.avro"
        size = path.stat().st_size
        existing = self._stat(key)
        if existing is None:
            self.client.fput_object(self.bucket, key, str(path),
                                   content_type="application/avro",
                                   metadata={"sha256": checksum})
        stored = self._stat(key)
        if (stored is None or stored.size != size
                or stored.metadata.get("x-amz-meta-sha256") != checksum):
            raise ValueError("Uploaded raw object failed size/checksum metadata verification")
        manifest = {
            "schema_version": 1, "status": "ready", "format": "opensky-raw-avro",
            "source": asdict(source), "source_id": source.identity,
            "bucket": self.bucket, "object_key": key, "sha256": checksum,
            "size_bytes": size, "imported_at": datetime.now(timezone.utc).isoformat(),
            **statistics,
        }
        payload = json.dumps(manifest, sort_keys=True, allow_nan=False).encode()
        self.client.put_object(self.bucket, self.manifest_key(source),
                               io.BytesIO(payload), len(payload), content_type="application/json")
        return manifest
