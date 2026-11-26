import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from airtraffic.ingestion.credentials import load_credentials


class CredentialTests(unittest.TestCase):
    def test_file_and_invalid_input_without_secret_in_error(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / 'credentials.json'
            path.write_text(json.dumps({'clientId': 'fixture-id', 'clientSecret': 'fixture-secret'}))
            self.assertEqual(load_credentials(path), ('fixture-id', 'fixture-secret'))
            for content in ('{fixture-secret', '[]', '{"clientSecret":"fixture-secret"}'):
                path.write_text(content)
                with self.assertRaises(ValueError) as caught:
                    load_credentials(path)
                self.assertNotIn('fixture-secret', str(caught.exception))
