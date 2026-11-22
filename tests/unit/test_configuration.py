"""Configuration reaches job entrypoints without connecting to services."""

import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

from airtraffic.jobs import producer
from airtraffic.jobs.stream import parse_args


class ConfigurationTests(unittest.TestCase):
    def test_environment_defaults_in_installed_package(self):
        settings = {
            "KAFKA_BOOTSTRAP_SERVERS": "broker.example:9092",
            "RAW_TOPIC": "fixture.raw",
            "RAW_ARCHIVE": "file:///tmp/bronze",
            "RAW_CHECKPOINT": "file:///tmp/archive-checkpoint",
            "STREAMING_ROOT": "file:///tmp/streaming",
            "RAW_AVRO": "/tmp/input.avro",
        }
        environment = {
            **os.environ,
            **{f"AIRTRAFFIC_{name}": value for name, value in settings.items()},
        }
        result = subprocess.run(
            [sys.executable, "-c", "from airtraffic import config; import json; "
             "print(json.dumps({k: v for k, v in vars(config).items() if k.isupper()}))"],
            cwd="/tmp", env=environment, check=True, capture_output=True, text=True,
        )
        self.assertEqual(json.loads(result.stdout), settings)

    def test_producer_cli_reaches_source_and_kafka(self):
        with (
            patch.object(producer, "replay_raw") as replay,
            patch.object(producer, "create_producer") as connect,
            patch.object(producer, "publish_records", return_value={"rows": 2}) as publish,
            contextlib.redirect_stdout(io.StringIO()) as output,
        ):
            result = producer.main([
                "--run-id", "config-fixture", "--topic", "cli.raw",
                "--bootstrap-servers", "cli-broker:9092",
                "--raw-avro", "/tmp/cli.avro", "--max-rows", "2",
            ])
        self.assertEqual(result, 0)
        connect.assert_called_once_with("cli-broker:9092")
        self.assertEqual(replay.call_args.args, (Path("/tmp/cli.avro"),))
        publish.assert_called_once_with(replay.return_value, connect.return_value, "cli.raw")
        connect.return_value.close.assert_called_once_with(timeout=35)
        self.assertEqual(json.loads(output.getvalue())["topic"], "cli.raw")

    def test_stream_cli_overrides_environment_defaults(self):
        arguments = parse_args([
            "--run-id", "config-fixture", "--version", "v1",
            "--bootstrap-servers", "cli-broker:9092", "--topic", "cli.raw",
            "--output-root", "file:///tmp/cli-streaming",
        ])
        self.assertEqual(arguments.bootstrap_servers, "cli-broker:9092")
        self.assertEqual(arguments.topic, "cli.raw")
        self.assertEqual(arguments.output_root, "file:///tmp/cli-streaming")


if __name__ == "__main__":
    unittest.main()
