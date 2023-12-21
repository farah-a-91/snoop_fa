import unittest
import pandas as pd
from snoop_fa.data_processing import json_to_dataframe


class TestJsonToDataFrame(unittest.TestCase):

    def test_json_to_dataframe(self):
        data = {'transactions': [
            {'currency': 'USD'},
            {'currency': 'EUR'},
            {'currency': 'JPY'},
        ]}
        df = json_to_dataframe(data)

        expected_df = pd.DataFrame([
            {'currency': 'USD'},
            {'currency': 'EUR'},
            {'currency': 'JPY'},
        ])
        pd.testing.assert_frame_equal(df, expected_df)


if __name__ == '__main__':
    unittest.main()
