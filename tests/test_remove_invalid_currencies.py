import unittest
import pandas as pd
from snoop_fa.data_processing import remove_invalid_currencies, json_to_dataframe


class TestRemoveInvalidCurrencies(unittest.TestCase):

    def test_remove_invalid_currencies(self):
        data = {'transactions': [
            {'currency': 'USD'},
            {'currency': 'EUR'},
            {'currency': 'JPY'},
        ]}
        df = json_to_dataframe(data)
        valid_currencies, invalid_currencies = remove_invalid_currencies(df)

        expected_valid = pd.DataFrame([
            {'currency': 'USD'},
            {'currency': 'EUR'},
        ])
        expected_invalid = pd.DataFrame([
            {'currency': 'JPY'},
        ])
        pd.testing.assert_frame_equal(valid_currencies, expected_valid)


if __name__ == '__main__':
    unittest.main()
