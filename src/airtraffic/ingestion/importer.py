"""Import each published source revision once; retry failures on the next poll."""

import logging
from pathlib import Path
from tempfile import TemporaryDirectory
import time

from airtraffic.ingestion.download import download_file
from airtraffic.ingestion.validation import inspect_raw_avro


LOGGER = logging.getLogger(__name__)


def import_once(files, store, *, max_bytes: int, work_dir: str | None = None) -> dict:
    counts = {"discovered": 0, "imported": 0, "skipped": 0, "failed": 0}
    for source in files:
        counts["discovered"] += 1
        try:
            if store.is_committed(source):
                counts["skipped"] += 1
                continue
            with TemporaryDirectory(prefix="airtraffic-import-", dir=work_dir) as directory:
                path = Path(directory) / "source.avro"
                checksum = download_file(source, path, max_bytes)
                statistics = inspect_raw_avro(path)
                store.commit(source, path, checksum, statistics)
            counts["imported"] += 1
            LOGGER.info("Imported source=%s rows=%s sha256=%s",
                        source.identity, statistics["record_count"], checksum)
        except Exception:
            counts["failed"] += 1
            LOGGER.exception("Import failed for source=%s; retry on next poll", source.identity)
    return counts


def poll(discover, store, *, interval: float, stop, once: bool = False,
         max_bytes: int = 1024 ** 3, work_dir: str | None = None) -> int:
    while not stop.is_set():
        started = time.monotonic()
        try:
            files = discover()
            counts = import_once(files, store, max_bytes=max_bytes, work_dir=work_dir)
            LOGGER.info("Poll complete: %s", counts)
            if not counts["discovered"]:
                LOGGER.warning("Source currently exposes no supported raw Avro files")
            exit_code = 1 if counts["failed"] else 0
        except Exception:
            LOGGER.exception("Cannot list source; retry on next poll")
            exit_code = 1
        if once:
            return exit_code
        stop.wait(max(1, interval - (time.monotonic() - started)))
    return 0
