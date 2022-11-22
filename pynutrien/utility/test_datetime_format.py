import pytest
from pynutrien.utility.datetime_format import *
import pandas as pd
from pandas.core.frame import DataFrame as pandas_data_frame

@pytest.fixture
def mock_pandas_df():
    date_range = pd.date_range('2022-11-04', periods=5, freq='D')
    data = {
        'id': [1, 2, 3, 4, 5],
        'letter_code': ['A','B','C','D','E'],
        'string_date': ['2022-11-04','2022-11-05','2022-11-06','2022-11-07','2022-11-08'],
        'date': date_range
        }
    df = pd.DataFrame(data)
    return df

@pytest.fixture
def desired_values():
    return [
        'November 04, 2022',
        'November 05, 2022',
        'November 06, 2022',
        'November 07, 2022',
        'November 08, 2022']

# test on panda df
def test_delegate_pandas_df_on_date_column_return_pandas_df(mock_pandas_df):
    new_df = delegate_df(df=mock_pandas_df, date_column='date', new_format='%B %d, %Y')
    assert isinstance(new_df, pandas_data_frame)

def test_delegate_pandas_df_on_date_column_return_correct_value(mock_pandas_df, desired_values):
    new_df = delegate_df(df=mock_pandas_df, date_column='date', new_format='%B %d, %Y')
    check_cond = new_df['date'] == desired_values
    assert check_cond.all() == True

def test_delegate_pandas_df_on_string_date_column_return_pandas_df(mock_pandas_df):
    new_df = delegate_df(df=mock_pandas_df, date_column='string_date', new_format='%B %d, %Y')
    assert isinstance(new_df, pandas_data_frame)

def test_delegate_pandas_df_on_string_date_column_return_correct_value(mock_pandas_df, desired_values):
    new_df = delegate_df(df=mock_pandas_df, date_column='string_date', new_format='%B %d, %Y')
    check_cond = new_df['string_date'] == desired_values
    assert check_cond.all() == True

def test_delegate_pandas_df_on_non_date_column_should_raise_value_error(mock_pandas_df):
    with pytest.raises(ValueError):
        new_df = delegate_df(df=mock_pandas_df, date_column='letter_code',  new_format='%B %d, %Y')



# test on spark df

# test on python object
