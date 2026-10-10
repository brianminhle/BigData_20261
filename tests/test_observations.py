"""Synthetic fixtures for the observation contract and historical adapter."""

from dataclasses import asdict, replace
import json
import unittest

from scripts.historical import normalize_historical
from scripts.observations import Observation, RejectedObservation, SourceReference
from scripts.sample_io import FIELDS


class ObservationTests(unittest.TestCase):
    def setUp(self):
        self.source = SourceReference("The OpenSky Network", "fixture", "https://example.org/sample.csv", "a" * 64, "sample.csv", 1)
        self.row = dict.fromkeys(FIELDS, "")
        self.row.update(
            time="1656302410", icao24=" 00ABCD ", lat="21", lon="105", velocity="0",
            heading="0", vertrate="-2", baroaltitude="-30.48", geoaltitude="1000",
            callsign=" TEST001 ", squawk="0042", onground="False", alert="0", spi="1",
            lastposupdate="1656302379.5", lastcontact="1656302409.5",
        )

    def normalize(self, row=None, **options):
        return normalize_historical(
            self.row if row is None else row, self.source,
            **{"run_id": "test-a", "received_at": 1800000000.25, **options},
        )

    def test_contract_keeps_identity_units_and_separate_clocks(self):
        record = self.normalize()
        self.assertIsInstance(record, Observation)
        self.assertEqual(record.schema_version, 1)
        self.assertEqual(record.mode, "replay")
        self.assertEqual(record.observation_id, "opensky:state:00abcd:1656302410")
        self.assertEqual(record.icao24, "00abcd")
        self.assertEqual(record.observed_at, 1656302410)
        self.assertEqual(record.position_updated_at, 1656302379.5)
        self.assertEqual(record.last_contact_at, 1656302409.5)
        self.assertEqual(record.received_at, 1800000000.25)
        self.assertEqual((record.latitude, record.longitude), (21, 105))
        self.assertEqual(record.barometric_altitude_m, -30.48)
        self.assertEqual(record.geometric_altitude_m, 1000)
        self.assertEqual(record.vertical_rate_mps, -2)
        self.assertEqual(record.callsign, "TEST001")
        self.assertEqual(record.squawk, "0042")
        self.assertEqual(record.quality_flags, ("stale_position",))
        self.assertEqual(record.provenance.record_index, 1)
        self.assertEqual(len(record.source_payload_sha256), 64)
        self.assertEqual(json.loads(json.dumps(asdict(record), allow_nan=False))["record_type"], "observation")

    def test_nulls_are_distinct_from_zero_and_false(self):
        record = self.normalize({**self.row, "lat": "\\N", "lon": "null", "geoaltitude": "None"})
        self.assertIsNone(record.latitude)
        self.assertIsNone(record.longitude)
        self.assertIsNone(record.geometric_altitude_m)
        self.assertEqual(record.ground_speed_mps, 0)
        self.assertEqual(record.track_deg, 0)
        self.assertIs(record.on_ground, False)
        self.assertIs(record.alert, False)
        self.assertIs(record.spi, True)
        self.assertIn("missing_position", record.quality_flags)

    def test_snapshot_identity_survives_retries_runs_and_dataset_overlap(self):
        first = self.normalize()
        retry = normalize_historical(
            self.row, replace(self.source, dataset_id="overlap", member="other.csv", record_index=99),
            run_id="test-b", received_at=1900000000,
        )
        conflict = self.normalize({**self.row, "lat": "22"})
        later = self.normalize({**self.row, "time": "1656302420"})
        self.assertEqual(first.observation_id, retry.observation_id)
        self.assertEqual(first.source_payload_sha256, retry.source_payload_sha256)
        self.assertEqual(first.observation_id, conflict.observation_id)
        self.assertNotEqual(first.source_payload_sha256, conflict.source_payload_sha256)
        self.assertNotEqual(first.observation_id, later.observation_id)

    def test_invalid_values_are_rejected_with_source_evidence(self):
        for field, text in (
            ("icao24", "xyz"), ("time", "1.5"), ("time", "0"),
            ("lat", "91"), ("lon", "-181"), ("velocity", "-1"),
            ("heading", "360"), ("geoaltitude", "NaN"), ("vertrate", "inf"),
            ("onground", "maybe"), ("squawk", "8888"),
        ):
            with self.subTest(field=field, text=text):
                record = self.normalize({**self.row, field: text})
                self.assertIsInstance(record, RejectedObservation)
                self.assertIn(f"invalid:{field}", record.reasons)
                self.assertEqual(record.raw_fields[field], text)
                self.assertEqual(record.provenance, self.source)
                json.dumps(asdict(record), allow_nan=False)

    def test_required_fields_and_malformed_rows_have_explicit_reasons(self):
        for field in ("icao24", "time"):
            record = self.normalize({**self.row, field: ""})
            self.assertIn(f"missing:{field}", record.reasons)
        for row in ({**self.row, "lon": None}, {**self.row, None: ["extra", "cells"]}):
            record = self.normalize(row)
            self.assertIn("malformed_row", record.reasons)
            self.assertEqual(record.extra_values, tuple(row.get(None, ())))
            self.assertNotIn(None, record.raw_fields)

    def test_unknown_columns_are_fingerprinted_and_remain_in_rejected_evidence(self):
        extra = self.normalize({**self.row, "new_field": "value"})
        self.assertNotEqual(extra.source_payload_sha256, self.normalize().source_payload_sha256)
        rejected = self.normalize({**self.row, "icao24": "bad", "new_field": "value"})
        self.assertEqual(rejected.raw_fields["new_field"], "value")

    def test_freshness_uses_source_time_and_keeps_future_or_missing_updates(self):
        record = self.normalize({**self.row, "lastposupdate": "", "lastcontact": "1656302410.5"})
        self.assertEqual(record.quality_flags, ("missing_position_time", "contact_time_after_snapshot"))
        record = self.normalize({**self.row, "lastposupdate": "1656302380", "lastcontact": ""})
        self.assertEqual(record.quality_flags, ("missing_contact_time",))
        self.assertEqual(self.normalize(stale_seconds=31).quality_flags, ())

    def test_invalid_run_clock_and_threshold_are_configuration_errors(self):
        for options in (
            {"run_id": " "}, {"received_at": float("nan")}, {"received_at": 0},
            {"stale_seconds": -1}, {"stale_seconds": float("inf")},
        ):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.normalize(**options)


if __name__ == "__main__":
    unittest.main()
