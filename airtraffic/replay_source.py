"""Load the verified, bounded historical input used by both replay destinations."""

from contextlib import closing
from pathlib import Path

from .observations import SourceReference
from .replay import ordered_prefix
from .sample_io import SampleError, iter_rows, verified_source


def load_replay_source(manifest_path: Path, max_rows: int):
    if not 1 <= max_rows <= 100000:
        raise SampleError("--max-rows must be between 1 and 100000 for this in-memory prototype")
    manifest, archive = verified_source(manifest_path)
    for key in ("provider", "dataset_id", "source_url"):
        if not isinstance(manifest.get(key), str) or not manifest[key].strip():
            raise SampleError(f"Manifest must contain {key}")
    source = SourceReference(
        provider=manifest["provider"], dataset_id=manifest["dataset_id"],
        source_url=manifest["source_url"], archive_sha256=manifest["sha256"],
        member="", record_index=0,
    )
    with closing(iter_rows(archive)) as rows:
        selected = ordered_prefix(rows, max_rows)
    return selected, source
