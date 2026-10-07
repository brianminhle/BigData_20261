"""Download and checksum the pinned OpenSky sample without changing its bytes."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
from http.client import HTTPException
import math
import os
from pathlib import Path
import re
import sys
import tempfile
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

if __package__:
    from .sample_io import DEFAULT_SOURCE, ROOT, SampleError, inspect_header, read_json, sha256_file, write_json
else:
    from sample_io import DEFAULT_SOURCE, ROOT, SampleError, inspect_header, read_json, sha256_file, write_json


def validate_source(source: dict) -> None:
    for key in ("dataset_id", "filename"):
        value = source.get(key, "")
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", value):
            raise SampleError(f"Source {key} must be a simple filename or identifier")
    url = urlparse(source.get("url", ""))
    if url.scheme != "https" or not url.netloc or url.username or url.password:
        raise SampleError("Source url must be a public HTTPS URL without credentials")
    size = source.get("expected_bytes")
    if not isinstance(size, int) or isinstance(size, bool) or size <= 0:
        raise SampleError("Source expected_bytes must be a positive integer")
    checksum = source.get("expected_sha256")
    if checksum is not None and (not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum)):
        raise SampleError("Source expected_sha256 must be null or a lowercase SHA-256 digest")


def verify_file(path: Path, size: int, checksum: str | None) -> str:
    if path.stat().st_size != size:
        raise SampleError(f"Size mismatch for {path.name}; keep the file for inspection and download to a fresh data directory")
    actual = sha256_file(path)
    if checksum and actual != checksum:
        raise SampleError(f"SHA-256 mismatch for {path.name}; keep the file for inspection and download to a fresh data directory")
    return actual


def download(source_path: Path, data_dir: Path, max_bytes: int, timeout: float) -> tuple[Path, bool]:
    source = read_json(source_path)
    validate_source(source)
    if max_bytes <= 0 or not math.isfinite(timeout) or timeout <= 0:
        raise SampleError("Download limit and timeout must be positive")
    if source["expected_bytes"] > max_bytes:
        raise SampleError("Source exceeds the download limit; increase --max-mib deliberately")
    target = data_dir / "raw" / source["filename"]
    manifest_path = data_dir / "manifests" / f"{source['dataset_id']}.json"
    if target.exists():
        if not manifest_path.exists():
            raise SampleError("Existing archive has no manifest; use a fresh --data-dir")
        manifest = read_json(manifest_path)
        if (manifest.get("source_url") != source["url"]
                or manifest.get("dataset_id") != source["dataset_id"]
                or (manifest_path.parent / manifest.get("local_path", "")).resolve() != target.resolve()):
            raise SampleError("Existing manifest does not match the requested source")
        if not re.fullmatch(r"[0-9a-f]{64}", str(manifest.get("sha256", ""))):
            raise SampleError("Existing manifest has no valid SHA-256 digest")
        actual = verify_file(target, source["expected_bytes"], source.get("expected_sha256"))
        if actual != manifest["sha256"] or target.stat().st_size != manifest.get("bytes"):
            raise SampleError("Existing archive does not match its manifest")
        inspect_header(target)
        return manifest_path, True
    if manifest_path.exists():
        raise SampleError("Manifest exists without its archive; use a fresh --data-dir")

    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        request = Request(source["url"], headers={"User-Agent": "AirTrafficCourseSample/0.1", "Accept-Encoding": "identity"})
        with urlopen(request, timeout=timeout) as response:
            if response.status != 200:
                raise SampleError(f"Expected a complete HTTP 200 response, received {response.status}")
            if urlparse(response.geturl()).scheme != "https":
                raise SampleError("Download redirected away from HTTPS")
            length = response.headers.get("Content-Length")
            if length is not None and int(length) != source["expected_bytes"]:
                raise SampleError("Server Content-Length does not match the pinned source size")
            digest = hashlib.sha256()
            count = 0
            with tempfile.NamedTemporaryFile(dir=target.parent, prefix=".download-", suffix="-" + target.name, delete=False) as stream:
                temporary = Path(stream.name)
                for block in iter(lambda: response.read(1024 * 1024), b""):
                    count += len(block)
                    if count > min(max_bytes, source["expected_bytes"]):
                        raise SampleError("Download exceeded its expected size or configured limit")
                    stream.write(block)
                    digest.update(block)
            if count != source["expected_bytes"]:
                raise SampleError(f"Incomplete download: expected {source['expected_bytes']} bytes, received {count}")
            checksum = digest.hexdigest()
            if source.get("expected_sha256") and checksum != source["expected_sha256"]:
                raise SampleError("Downloaded bytes do not match the pinned SHA-256 digest")
            inspect_header(temporary)
            manifest = {
                "manifest_version": 1,
                "dataset_id": source["dataset_id"],
                "provider": source.get("provider"),
                "source_url": source["url"],
                "resolved_url": response.geturl(),
                "documentation_url": source.get("documentation_url"),
                "schema_url": source.get("schema_url"),
                "terms_url": source.get("terms_url"),
                "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
                "bytes": count,
                "sha256": checksum,
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "local_path": os.path.relpath(target.resolve(), manifest_path.parent.resolve()),
            }
        # Never replace an existing archive, even if another process completed first.
        os.link(temporary, target)
        try:
            write_json(manifest_path, manifest)
        except Exception:
            target.unlink()
            raise
        return manifest_path, False
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE, help="Source JSON (default: pinned OpenSky hour)")
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data", help="Directory for raw/ and manifests/")
    parser.add_argument("--max-mib", type=int, default=256, help="Maximum download size in MiB (default: 256)")
    parser.add_argument("--timeout", type=float, default=30, help="Network operation timeout in seconds (default: 30)")
    args = parser.parse_args()
    print("Preparing the sample: verifying an existing archive or downloading it. A new download may take a few minutes.", flush=True)
    try:
        path, cached = download(args.source, args.data_dir, args.max_mib * 1024 * 1024, args.timeout)
    except (OSError, ValueError, URLError, HTTPException) as exc:
        print(f"Download failed: {exc}", file=sys.stderr)
        return 1
    print(f"{'Verified cached archive' if cached else 'Downloaded and verified archive'}; manifest: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
