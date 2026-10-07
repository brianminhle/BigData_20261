"""Shared, dependency-free readers for OpenSky CSV samples.

Archive members are streamed, never extracted onto the filesystem.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import tarfile
import tempfile
from typing import Iterator

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT / "data/sources/opensky_sample.json"
DEFAULT_MANIFEST = ROOT / "data/manifests/opensky_states_2022-06-27_04.json"
FIELDS = (
    "time", "icao24", "lat", "lon", "velocity", "heading", "vertrate",
    "callsign", "onground", "alert", "spi", "squawk", "baroaltitude",
    "geoaltitude", "lastposupdate", "lastcontact",
)


class SampleError(ValueError):
    """An actionable input, integrity, or format error."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise SampleError(f"Expected a JSON object in {path}")
    return value


def write_json(path: Path, value: dict) -> None:
    """Publish complete JSON atomically, including when replacing a report."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, suffix=".part", delete=False,
        ) as stream:
            temporary = Path(stream.name)
            json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
            stream.write("\n")
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _csv_rows(binary: io.BufferedIOBase, member: str) -> Iterator[tuple[str, int, dict]]:
    with io.TextIOWrapper(binary, encoding="utf-8-sig", newline="") as text:
        reader = csv.DictReader(text, strict=True)
        columns = reader.fieldnames or []
        missing = sorted(set(FIELDS) - set(columns))
        if missing:
            raise SampleError(f"{member}: missing OpenSky columns: {', '.join(missing)}")
        if len(columns) != len(set(columns)):
            raise SampleError(f"{member}: duplicate CSV column names")
        for index, row in enumerate(reader, start=1):
            yield member, index, row


def iter_rows(path: Path) -> Iterator[tuple[str, int, dict]]:
    """Read CSV, CSV.GZ, or TAR archives containing either, in source order."""
    name = path.name.lower()
    try:
        if name.endswith((".tar", ".tar.gz", ".tgz")):
            found = False
            # The archive is a local file. Seekable mode lets TextIOWrapper read
            # plain CSV members on Python 3.10–3.12; payloads remain streamed.
            with tarfile.open(path, mode="r:*") as archive:
                for member in archive:
                    if not member.name.lower().endswith((".csv", ".csv.gz")):
                        continue
                    if not member.isfile():
                        raise SampleError(f"CSV archive member is not a regular file: {member.name}")
                    found = True
                    binary = archive.extractfile(member)
                    assert binary is not None
                    with binary:
                        if member.name.lower().endswith(".gz"):
                            with gzip.GzipFile(fileobj=binary) as decoded:
                                yield from _csv_rows(decoded, member.name)
                        else:
                            yield from _csv_rows(binary, member.name)
            if not found:
                raise SampleError("Archive contains no CSV or CSV.GZ files")
        elif name.endswith(".csv.gz"):
            with gzip.open(path, "rb") as binary:
                yield from _csv_rows(binary, path.name)
        elif name.endswith(".csv"):
            with path.open("rb") as binary:
                yield from _csv_rows(binary, path.name)
        else:
            raise SampleError("Supported input formats: .csv, .csv.gz, .tar, .tar.gz, .tgz")
    except (tarfile.TarError, gzip.BadGzipFile, EOFError, UnicodeError, csv.Error) as exc:
        raise SampleError(f"Cannot read {path.name}: {exc}") from exc


def inspect_header(path: Path) -> None:
    rows = iter_rows(path)
    try:
        if next(rows, None) is None:
            raise SampleError("The sample contains a header but no observations")
    finally:
        rows.close()
