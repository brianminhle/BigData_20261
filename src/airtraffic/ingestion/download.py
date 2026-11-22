"""Bounded download of a single, stable source object."""

from pathlib import Path
from urllib.request import Request, urlopen

from airtraffic.ingestion.catalog import SourceFile
from airtraffic.ingestion.source import sha256_file


def download_file(source: SourceFile, target: Path, max_bytes: int) -> str:
    if source.size is not None and source.size > max_bytes:
        raise ValueError("Source file exceeds the configured size limit")
    headers = {"User-Agent": "airtraffic-raw-importer/0.1", "Accept-Encoding": "identity"}
    if source.etag:
        etag = source.etag.strip('"')
        headers["If-Match"] = f'"{etag}"'
    request = Request(source.url, headers=headers)
    with urlopen(request, timeout=60) as response, target.open("wb") as output:
        advertised_size = response.headers.get("Content-Length")
        expected_size = int(advertised_size) if advertised_size is not None else None
        if expected_size is not None and expected_size > max_bytes:
            raise ValueError("Download exceeds the configured size limit")
        if source.etag and response.headers.get("ETag", "").strip('"') != source.etag.strip('"'):
            raise ValueError("Source ETag changed between listing and download")
        size = 0
        while block := response.read(1024 * 1024):
            size += len(block)
            if size > max_bytes:
                raise ValueError("Download exceeds the configured size limit")
            output.write(block)
    if size == 0 or any(value is not None and size != value
                        for value in (source.size, expected_size)):
        raise ValueError("Empty or incomplete source download")
    checksum = sha256_file(target)
    if source.expected_sha256 and checksum != source.expected_sha256:
        raise ValueError("Source SHA-256 mismatch")
    return checksum
