import unittest
from airtraffic.serving.query import query_view


class QueryTests(unittest.TestCase):
    def setUp(self):
        self.view = dict(generated_at=900, latest_snapshot=650,
                         windows=[dict(scope_id='a', window_start=300, observations=4,
                                       with_position=3, stale_positions=1, distinct_aircraft=2),
                                  dict(scope_id='a', window_start=600, observations=5,
                                       with_position=5, stale_positions=0, distinct_aircraft=3)],
                         cells=[dict(scope_id='a', window_start=300, distinct_aircraft=2),
                                dict(scope_id='a', window_start=300, distinct_aircraft=2)])

    def test_distinct_is_not_summed_across_cells_or_time(self):
        result = query_view(self.view, {})
        self.assertEqual(result['summary']['peak_window_aircraft'], 3)
        self.assertEqual(result['summary']['observations'], 9)

    def test_half_open_aligned_time_filter(self):
        result = query_view(self.view, {'start':['300'], 'end':['600']})
        self.assertEqual(result['summary']['observations'], 4)
        self.assertEqual(len(result['cells']), 2)

    def test_invalid_filters(self):
        for params in ({'scope':['missing']}, {'start':['x']}, {'start':['301']}, {'end':['0']}):
            with self.subTest(params=params), self.assertRaises(ValueError):
                query_view(self.view, params)

    def test_empty_is_not_zero_aircraft(self):
        result = query_view(self.view, {'start':['900']})
        self.assertIsNone(result['summary']['peak_window_aircraft'])
        self.assertEqual(result['cells'], [])

    def test_relative_range_uses_latest_export(self):
        self.view['latest_snapshot'] = 4500
        result = query_view(self.view, {'hours':['1']})
        self.assertEqual(result['windows'], [])
        with self.assertRaises(ValueError):
            query_view(self.view, {'hours':['0']})
