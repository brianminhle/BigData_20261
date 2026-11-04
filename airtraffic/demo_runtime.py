"""Run Compose commands and wait for a checkpoint run to reach the archive."""

import json
import os
from pathlib import Path
import subprocess
import time
from uuid import uuid4

from .sample_io import ROOT


class DemoError(RuntimeError):
    """A failed checkpoint step with its command output retained in the log."""


def run_compose(arguments: list[str], log_path: Path, timeout: float = 180):
    container_name = f"airtraffic-demo-{uuid4().hex[:12]}" if arguments[0] == "run" else None
    command = ["docker", "compose", *arguments]
    if container_name:
        command[3:3] = ["--name", container_name]
    environment = os.environ.copy()
    if hasattr(os, "getuid"):
        environment.setdefault("LOCAL_UID", str(os.getuid()))
        environment.setdefault("LOCAL_GID", str(os.getgid()))
    with log_path.open("a", encoding="utf-8") as log:
        log.write("\n$ " + " ".join(command) + "\n")
        log.flush()
        try:
            result = subprocess.run(
                command, cwd=ROOT, env=environment, capture_output=True,
                text=True, timeout=timeout, check=False,
            )
        except subprocess.TimeoutExpired as exc:
            for captured in (exc.stdout, exc.stderr):
                if captured:
                    log.write(captured.decode("utf-8", errors="replace") if isinstance(captured, bytes) else captured)
            log.write(f"Command timed out after {timeout:.1f} seconds\n")
            _remove_container(container_name, log)
            raise DemoError("Compose command timed out; inspect the services and demo.log") from exc
        except KeyboardInterrupt:
            _remove_container(container_name, log)
            raise
        log.write(result.stdout)
        log.write(result.stderr)
    return result


def _remove_container(name, log):
    if name is None:
        return
    try:
        result = subprocess.run(["docker", "rm", "-f", name], capture_output=True, text=True, timeout=15)
        log.write(f"Stopped timed-out/interrupted container {name}: {result.returncode}\n")
    except (OSError, subprocess.TimeoutExpired):
        log.write(f"Could not stop {name}; inspect it with docker logs and docker stop\n")


def result_json(result) -> dict:
    for line in reversed(result.stdout.splitlines()):
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict):
            return value
    raise DemoError("Command did not produce a JSON result; inspect demo.log")


def checked_compose(arguments: list[str], log_path: Path, timeout: float = 180):
    result = run_compose(arguments, log_path, timeout)
    if result.returncode:
        raise DemoError(f"Compose step failed (exit {result.returncode}): {' '.join(arguments)}")
    return result


def wait_for_archive(run_id: str, log_path: Path, timeout_seconds: float, *, expected_rows=10, path=None) -> dict:
    deadline = time.monotonic() + timeout_seconds
    while (remaining := deadline - time.monotonic()) > 0:
        arguments = ["run", "--rm", "inspect", "--run-id", run_id, "--expect-rows", str(expected_rows)]
        if path:
            arguments.extend(["--path", path])
        result = run_compose(
            arguments,
            log_path, timeout=min(90, remaining),
        )
        if result.returncode == 0:
            return result_json(result)
        try:
            summary = result_json(result)
        except DemoError:
            summary = {}
        if summary.get("rows", 0) >= expected_rows:
            raise DemoError("Archive verification failed; choose a new run ID and inspect demo.log")
        time.sleep(min(2, max(0, deadline - time.monotonic())))
    raise DemoError(f"Archive did not reach {expected_rows} records before the deadline; inspect demo.log")
