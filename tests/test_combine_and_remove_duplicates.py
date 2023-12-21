import unittest
import pandas as pd
from snoop_fa.data_processing import combine_and_remove_duplicates


class TestCombineAndRemoveDuplicates(unittest.TestCase):

    def test_combine_and_remove_duplicates(self):
        valid_dfs = [
            pd.DataFrame({'id': [1, 2], 'value': ['A', 'B']}),
            pd.DataFrame({'id': [3, 4], 'value': ['C', 'D']}),
        ]
        invalid_dfs = [
            pd.DataFrame({'id': [5, 6], 'value': ['E', 'F']})
        ]

        combined_valid_df, combined_invalid_df = combine_and_remove_duplicates(valid_dfs, invalid_dfs)

        expected_combined_valid_df = pd.DataFrame({'id': [1, 2, 3, 4], 'value': ['A', 'B', 'C', 'D']})
        expected_combined_invalid_df = pd.DataFrame({'id': [5, 6], 'value': ['E', 'F']})
        pd.testing.assert_frame_equal(combined_valid_df, expected_combined_valid_df)
        pd.testing.assert_frame_equal(combined_invalid_df, expected_combined_invalid_df)


if __name__ == '__main__':
    unittest.main()
