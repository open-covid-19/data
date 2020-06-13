# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import cProfile
from io import StringIO
from pstats import Stats
from unittest import TestCase, main

from pandas import DataFrame, isnull
from lib.cast import age_group
from lib.utils import combine_tables, stack_table

# Synthetic data used for testing
COMBINE_TEST_DATA_1 = DataFrame.from_records(
    [
        # Both values are null
        {"key": "A", "value_column_1": None, "value_column_2": None},
        # Left value is null
        {"key": "A", "value_column_1": None, "value_column_2": 1},
        # Right value is null
        {"key": "A", "value_column_1": 1, "value_column_2": None},
        # No value is null
        {"key": "A", "value_column_1": 1, "value_column_2": 1},
    ]
)
COMBINE_TEST_DATA_2 = DataFrame.from_records(
    [
        # Both values are null
        {"key": "A", "value_column_1": None, "value_column_2": None},
        # Left value is null
        {"key": "A", "value_column_1": None, "value_column_2": 2},
        # Right value is null
        {"key": "A", "value_column_1": 2, "value_column_2": None},
        # No value is null
        {"key": "A", "value_column_1": 2, "value_column_2": 2},
    ]
)
STACK_TEST_DATA = DataFrame.from_records(
    [
        {"idx": 0, "piv": "A", "val": 1},
        {"idx": 0, "piv": "A", "val": 2},
        {"idx": 1, "piv": "B", "val": 3},
        {"idx": 1, "piv": "B", "val": 4},
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
        data1 = COMBINE_TEST_DATA_1.copy()
        data2 = COMBINE_TEST_DATA_2.copy()
        result = combine_tables([data1[0:1], data2[0:1]], ["key"])
        self.assertEqual(1, len(result))
        self.assertTrue(isnull(result.loc[0, "value_column_1"]))
        self.assertTrue(isnull(result.loc[0, "value_column_2"]))

    def test_combine_first_both_none(self):
        data1 = COMBINE_TEST_DATA_1.copy()
        data2 = COMBINE_TEST_DATA_2.copy()
        result = combine_tables([data1[0:1], data2[3:4]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual(2, result.loc[0, "value_column_1"])
        self.assertEqual(2, result.loc[0, "value_column_2"])

    def test_combine_second_both_none(self):
        data1 = COMBINE_TEST_DATA_1.copy()
        data2 = COMBINE_TEST_DATA_2.copy()
        result = combine_tables([data1[3:4], data2[0:1]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual(1, result.loc[0, "value_column_1"])
        self.assertEqual(1, result.loc[0, "value_column_2"])

    def test_combine_first_left_none(self):
        data1 = COMBINE_TEST_DATA_1.copy()
        data2 = COMBINE_TEST_DATA_2.copy()
        result = combine_tables([data1[1:2], data2[3:4]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual(2, result.loc[0, "value_column_1"])
        self.assertEqual(2, result.loc[0, "value_column_2"])

    def test_combine_first_right_none(self):
        data1 = COMBINE_TEST_DATA_1.copy()
        data2 = COMBINE_TEST_DATA_2.copy()
        result = combine_tables([data1[2:3], data2[3:4]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual(2, result.loc[0, "value_column_1"])
        self.assertEqual(2, result.loc[0, "value_column_2"])

    def test_combine_second_left_none(self):
        data1 = COMBINE_TEST_DATA_1.copy()
        data2 = COMBINE_TEST_DATA_2.copy()
        result = combine_tables([data1[3:4], data2[1:2]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual(1, result.loc[0, "value_column_1"])
        self.assertEqual(2, result.loc[0, "value_column_2"])

    def test_combine_second_right_none(self):
        data1 = COMBINE_TEST_DATA_1.copy()
        data2 = COMBINE_TEST_DATA_2.copy()
        result = combine_tables([data1[3:4], data2[2:3]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual(2, result.loc[0, "value_column_1"])
        self.assertEqual(1, result.loc[0, "value_column_2"])

    def test_stack_data(self):
        data = DataFrame.from_records(
            [
                {"idx": 0, "piv": "A", "val": 1},
                {"idx": 0, "piv": "B", "val": 2},
                {"idx": 1, "piv": "A", "val": 3},
                {"idx": 1, "piv": "B", "val": 4},
            ]
        )

        expected = DataFrame.from_records(
            [
                {"idx": 0, "val": 3, "val_A": 1, "val_B": 2},
                {"idx": 1, "val": 7, "val_A": 3, "val_B": 4},
            ]
        )

        buffer1 = StringIO()
        buffer2 = StringIO()

        expected.to_csv(buffer1)
        stack_table(
            data, index_columns=["idx"], value_columns=["val"], stack_columns=["piv"]
        ).to_csv(buffer2)

        self.assertEqual(buffer1.getvalue(), buffer2.getvalue())

    def test_age_group(self):
        self.assertEqual("0-9", age_group(0, bin_count=10, max_age=100))
        self.assertEqual("0-9", age_group(0.0, bin_count=10, max_age=100))
        self.assertEqual("0-9", age_group(9, bin_count=10, max_age=100))
        self.assertEqual("10-19", age_group(10, bin_count=10, max_age=100))
        self.assertEqual("10-19", age_group(19, bin_count=10, max_age=100))
        self.assertEqual("90-", age_group(90, bin_count=10, max_age=100))
        self.assertEqual("90-", age_group(100, bin_count=10, max_age=100))
        self.assertEqual("90-", age_group(1e9, bin_count=10, max_age=100))
        self.assertEqual(None, age_group(-1, bin_count=10, max_age=100))

    # TODO: Add test for stratify_age_and_sex


if __name__ == "__main__":
    sys.exit(main())
