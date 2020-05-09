import sys
import cProfile
from pstats import Stats
from unittest import TestCase, main

from pandas import DataFrame
from lib.default_pipeline import DefaultPipeline

# Synthetic data used for testing
TEST_AUX_DATA = DataFrame.from_records([
    # Country with no subregions
    {'key': 'AA', 'country_code': 'AA', 'subregion_1_code': None, 'subregion_2_code': None},
    # Country with one level-1 subregion
    {'key': 'AB', 'country_code': 'AB', 'subregion_1_code': None, 'subregion_2_code': None},
    {'key': 'AB_1', 'country_code': 'AB', 'subregion_1_code': '1', 'subregion_2_code': None},
    # Country with five level-1 subregions
    {'key': 'AC', 'country_code': 'AC', 'subregion_1_code': None, 'subregion_2_code': None},
    {'key': 'AC_1', 'country_code': 'AC', 'subregion_1_code': '1', 'subregion_2_code': None},
    {'key': 'AC_2', 'country_code': 'AC', 'subregion_1_code': '2', 'subregion_2_code': None},
    {'key': 'AC_3', 'country_code': 'AC', 'subregion_1_code': '3', 'subregion_2_code': None},
    {'key': 'AC_4', 'country_code': 'AC', 'subregion_1_code': '4', 'subregion_2_code': None},
    {'key': 'AC_5', 'country_code': 'AC', 'subregion_1_code': '5', 'subregion_2_code': None},
    # Country with one level-1 subregion and one level-2 subregion
    {'key': 'AD', 'country_code': 'AD', 'subregion_1_code': None, 'subregion_2_code': None},
    {'key': 'AD_1', 'country_code': 'AD', 'subregion_1_code': '1', 'subregion_2_code': None},
    {'key': 'AD_1_1', 'country_code': 'AD', 'subregion_1_code': '1', 'subregion_2_code': '1'},
    # Country with one level-1 subregion and five level-2 subregions
    {'key': 'AE', 'country_code': 'AE', 'subregion_1_code': None, 'subregion_2_code': None},
    {'key': 'AE_1', 'country_code': 'AE', 'subregion_1_code': '1', 'subregion_2_code': None},
    {'key': 'AE_1_1', 'country_code': 'AE', 'subregion_1_code': '1', 'subregion_2_code': '1'},
    {'key': 'AE_1_2', 'country_code': 'AE', 'subregion_1_code': '1', 'subregion_2_code': '2'},
    {'key': 'AE_1_3', 'country_code': 'AE', 'subregion_1_code': '1', 'subregion_2_code': '3'},
    {'key': 'AE_1_4', 'country_code': 'AE', 'subregion_1_code': '1', 'subregion_2_code': '4'},
    {'key': 'AE_1_5', 'country_code': 'AE', 'subregion_1_code': '1', 'subregion_2_code': '5'},
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
        record = {'country_code': '__'}
        key = pipeline.merge(record, aux)
        self.assertTrue(key is None)

    def test_merge_zero_subregions(self):
        aux = TEST_AUX_DATA.copy()
        pipeline = DefaultPipeline()
        record = {'country_code': 'AA'}
        key = pipeline.merge(record, aux)
        self.assertEqual(key, 'AA')

    def test_merge_one_subregion(self):
        aux = TEST_AUX_DATA.copy()
        pipeline = DefaultPipeline()

        record = {'country_code': 'AB'}
        key = pipeline.merge(record, aux)
        self.assertTrue(key is None)

        record = {'country_code': 'AB', 'subregion_1_code': None}
        key = pipeline.merge(record, aux)
        self.assertEqual(key, 'AB')

        record = {'country_code': 'AB', 'subregion_1_code': '1'}
        key = pipeline.merge(record, aux)
        self.assertEqual(key, 'AB_1')


if __name__ == '__main__':
    sys.exit(main())
