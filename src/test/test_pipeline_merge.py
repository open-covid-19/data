import sys
import cProfile
from pstats import Stats
from unittest import TestCase, main

from pandas import DataFrame
from lib.default_pipeline import DefaultPipeline

# Synthetic data used for testing
TEST_AUX_DATA = DataFrame.from_records([
    # Country with no subregions
    {'key': 'AA', 'country': 'AA', 'subregion_1': None, 'subregion_2': None},
    # Country with one level-1 subregion
    {'key': 'AB', 'country': 'AB', 'subregion_1': None, 'subregion_2': None},
    {'key': 'AB_1', 'country': 'AB', 'subregion_1': '1', 'subregion_2': None},
    # Country with five level-1 subregions
    {'key': 'AC', 'country': 'AC', 'subregion_1': None, 'subregion_2': None},
    {'key': 'AC_1', 'country': 'AC', 'subregion_1': '1', 'subregion_2': None},
    {'key': 'AC_2', 'country': 'AC', 'subregion_1': '2', 'subregion_2': None},
    {'key': 'AC_3', 'country': 'AC', 'subregion_1': '3', 'subregion_2': None},
    {'key': 'AC_4', 'country': 'AC', 'subregion_1': '4', 'subregion_2': None},
    {'key': 'AC_5', 'country': 'AC', 'subregion_1': '5', 'subregion_2': None},
    # Country with one level-1 subregion and one level-2 subregion
    {'key': 'AD', 'country': 'AD', 'subregion_1': None, 'subregion_2': None},
    {'key': 'AD_1', 'country': 'AD', 'subregion_1': '1', 'subregion_2': None},
    {'key': 'AD_1_1', 'country': 'AD', 'subregion_1': '1', 'subregion_2': '1'},
    # Country with one level-1 subregion and five level-2 subregions
    {'key': 'AE', 'country': 'AE', 'subregion_1': None, 'subregion_2': None},
    {'key': 'AE_1', 'country': 'AE', 'subregion_1': '1', 'subregion_2': None},
    {'key': 'AE_1_1', 'country': 'AE', 'subregion_1': '1', 'subregion_2': '1'},
    {'key': 'AE_1_2', 'country': 'AE', 'subregion_1': '1', 'subregion_2': '2'},
    {'key': 'AE_1_3', 'country': 'AE', 'subregion_1': '1', 'subregion_2': '3'},
    {'key': 'AE_1_4', 'country': 'AE', 'subregion_1': '1', 'subregion_2': '4'},
    {'key': 'AE_1_5', 'country': 'AE', 'subregion_1': '1', 'subregion_2': '5'},
])


class TestPipelineMerge(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.profiler = cProfile.Profile()
        cls.profiler.enable()

    @classmethod
    def tearDownClass(cls):
        stats = Stats(cls.profiler)
        stats.strip_dirs()
        stats.sort_stats('cumtime')
        stats.print_stats(20)

    def test_merge_no_match(self):
        aux = TEST_AUX_DATA.copy()
        pipeline = DefaultPipeline()
        record = {'country': '__'}
        key = pipeline.merge(record, aux)
        self.assertTrue(key is None)

    def test_merge_zero_subregions(self):
        aux = TEST_AUX_DATA.copy()
        pipeline = DefaultPipeline()
        record = {'country': 'AA'}
        key = pipeline.merge(record, aux)
        self.assertEqual(key, 'AA')

    def test_merge_one_subregion(self):
        aux = TEST_AUX_DATA.copy()
        pipeline = DefaultPipeline()

        record = {'country': 'AB'}
        key = pipeline.merge(record, aux)
        self.assertTrue(key is None)

        record = {'country': 'AB', 'subregion_1': None}
        key = pipeline.merge(record, aux)
        self.assertEqual(key, 'AB')

        record = {'country': 'AB', 'subregion_1': '1'}
        key = pipeline.merge(record, aux)
        self.assertEqual(key, 'AB_1')


if __name__ == '__main__':
    sys.exit(main())
