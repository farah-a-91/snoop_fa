import pandas as pd


def json_to_dataframe(data):
    """
    Normalize JSON data to a Pandas DataFrame.
    """
    return pd.json_normalize(data['transactions'])


def remove_invalid_currencies(df):
    """
    Remove transactions with invalid currencies from a DataFrame.
    """
    currencies_to_check = ['EUR', 'GBP', 'USD']
    valid_currencies_df = df[df['currency'].isin(currencies_to_check)]
    invalid_currencies_df = df[~df['currency'].isin(currencies_to_check)]
    return valid_currencies_df, invalid_currencies_df


def remove_invalid_dates(df):
    """
    Remove transactions with invalid dates from a DataFrame.
    """
    df = df.copy()  # Make a copy to avoid the SettingWithCopyWarning
    df['transactionDate'] = pd.to_datetime(df['transactionDate'], errors='coerce')
    valid_dates_df = df.dropna(subset=['transactionDate'])
    invalid_dates_df = df[df['transactionDate'].isnull()]
    return valid_dates_df, invalid_dates_df


def remove_duplicates(df):
    """
    Remove duplicate transactions from a DataFrame.
    """
    duplicates_mask = df.duplicated()
    valid_duplicates_df = df[~duplicates_mask].reset_index(drop=True)
    invalid_duplicates_df = df[duplicates_mask].reset_index(drop=True)
    return valid_duplicates_df, invalid_duplicates_df


def combine_and_remove_duplicates(valid_dfs, invalid_dfs):
    """
    Combine and remove duplicates from valid and invalid DataFrames.
    """
    combined_valid_df = pd.concat(valid_dfs, ignore_index=True)
    combined_invalid_df = pd.concat(invalid_dfs, ignore_index=True)
    combined_valid_df = combined_valid_df.drop_duplicates()
    combined_invalid_df = combined_invalid_df.drop_duplicates()
    return combined_valid_df, combined_invalid_df
