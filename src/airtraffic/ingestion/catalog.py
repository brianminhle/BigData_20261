"""Discover raw Avro files without treating publication time as event time."""

from dataclasses import asdict, dataclass
import hashlib
import json
from urllib.parse import quote, urlsplit

from airtraffic.ingestion.source import SAMPLE_SHA256, SAMPLE_URL


@dataclass(frozen=True)
class SourceFile:
    dataset: str
    url: str
    size: int | None = None
    etag: str | None = None
    last_modified: str | None = None
    expected_sha256: str | None = None

    @property
    def identity(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True).encode()
        return hashlib.sha256(payload).hexdigest()


def sample_files() -> list[SourceFile]:
    return [SourceFile("opensky_raw_20150421", SAMPLE_URL,
                       expected_sha256=SAMPLE_SHA256)]


def s3_files(endpoint: str, bucket: str, prefix: str, dataset: str) -> list[SourceFile]:
    from minio import Minio
    from urllib3 import PoolManager, Timeout

    address = urlsplit(endpoint)
    if (address.scheme not in {"https", "http"} or not address.netloc
            or address.path not in {"", "/"} or address.query or address.fragment
            or address.username or address.password):
        raise ValueError("Source endpoint must be an HTTP(S) origin without credentials")
    client = Minio(address.netloc, secure=address.scheme == "https",
                   http_client=PoolManager(timeout=Timeout(connect=10, read=60), retries=2))
    files = []
    for item in client.list_objects(bucket, prefix=prefix, recursive=True):
        if not item.object_name.endswith(".avro") or not item.size:
            continue
        files.append(SourceFile(
            dataset=dataset,
            url=f"{endpoint.rstrip('/')}/{quote(bucket, safe='')}/{quote(item.object_name, safe='/')}",
            size=item.size, etag=item.etag,
            last_modified=item.last_modified.isoformat() if item.last_modified else None,
        ))
    return files
