import io
import unittest
from contextlib import redirect_stderr
from airtraffic.analytics.config import parse_args


class AnalyticsConfigTests(unittest.TestCase):
    def test_run_requires_aligned_bounds_and_valid_thresholds(self):
        base = ['--output','/tmp/run','--start','0','--end','300']
        self.assertEqual(parse_args(base).gap_seconds, 600)
        for option in (['--start','1'], ['--end','0'], ['--max-speed-mps','nan'],
                       ['--gap-seconds','0'], ['--shuffle-partitions','0']):
            with self.subTest(option=option), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parse_args(base + option)
