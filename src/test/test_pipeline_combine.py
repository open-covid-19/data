import sys
import cProfile
from pstats import Stats
from unittest import TestCase, main

from pandas import DataFrame
from lib.utils import combine_tables

# Synthetic data used for testing
TEST_DATA_1 = DataFrame.from_records(
    [
        # Both values are null
        {"key": "A", "value_column_1": None, "value_column_2": None,},
        # Left value is null
        {"key": "A", "value_column_1": None, "value_column_2": '1',},
        # Right value is null
        {"key": "A", "value_column_1": '1', "value_column_2": None,},
        # No value is null
        {"key": "A", "value_column_1": "1", "value_column_2": "1",},
    ]
)
TEST_DATA_2 = DataFrame.from_records(
    [
        # Both values are null
        {"key": "A", "value_column_1": None, "value_column_2": None,},
        # Left value is null
        {"key": "A", "value_column_1": None, "value_column_2": "2",},
        # Right value is null
        {"key": "A", "value_column_1": "2", "value_column_2": None,},
        # No value is null
        {"key": "A", "value_column_1": "2", "value_column_2": "2",},
    ]
)


class TestPipelineMerge(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.profiler = cProfile.Profile()
        cls.profiler.enable()

    @classmethod
    def tearDownClass(cls):
        stats = Stats(cls.profiler)
        stats.strip_dirs()
        stats.sort_stats("cumtime")
        stats.print_stats(20)

    def test_combine_all_none(self):
        data1 = TEST_DATA_1.copy()
        data2 = TEST_DATA_2.copy()
        result = combine_tables([data1[0:1], data2[0:1]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual(None, result.loc[0, "value_column_1"])
        self.assertEqual(None, result.loc[0, "value_column_2"])

    def test_combine_first_both_none(self):
        data1 = TEST_DATA_1.copy()
        data2 = TEST_DATA_2.copy()
        result = combine_tables([data1[0:1], data2[3:4]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual("2", result.loc[0, "value_column_1"])
        self.assertEqual("2", result.loc[0, "value_column_2"])

    def test_combine_second_both_none(self):
        data1 = TEST_DATA_1.copy()
        data2 = TEST_DATA_2.copy()
        result = combine_tables([data1[3:4], data2[0:1]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual("1", result.loc[0, "value_column_1"])
        self.assertEqual("1", result.loc[0, "value_column_2"])

    def test_combine_first_left_none(self):
        data1 = TEST_DATA_1.copy()
        data2 = TEST_DATA_2.copy()
        result = combine_tables([data1[1:2], data2[3:4]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual("2", result.loc[0, "value_column_1"])
        self.assertEqual("2", result.loc[0, "value_column_2"])

    def test_combine_first_right_none(self):
        data1 = TEST_DATA_1.copy()
        data2 = TEST_DATA_2.copy()
        result = combine_tables([data1[2:3], data2[3:4]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual("2", result.loc[0, "value_column_1"])
        self.assertEqual("2", result.loc[0, "value_column_2"])

    def test_combine_second_left_none(self):
        data1 = TEST_DATA_1.copy()
        data2 = TEST_DATA_2.copy()
        result = combine_tables([data1[3:4], data2[1:2]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual("1", result.loc[0, "value_column_1"])
        self.assertEqual("2", result.loc[0, "value_column_2"])

    def test_combine_second_right_none(self):
        data1 = TEST_DATA_1.copy()
        data2 = TEST_DATA_2.copy()
        result = combine_tables([data1[3:4], data2[2:3]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual("2", result.loc[0, "value_column_1"])
        self.assertEqual("1", result.loc[0, "value_column_2"])


if __name__ == "__main__":
    sys.exit(main())
