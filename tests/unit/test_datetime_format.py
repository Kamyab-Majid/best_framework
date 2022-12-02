from __future__ import annotations

from datetime import date

import pandas as pd
import pyspark.sql.types as T
import pytest
from pandas import DataFrame as pandas_data_frame
from pyspark.sql import DataFrame as spark_data_frame
from pyspark.sql import SparkSession

from pynutrien.util.datetime_format import delegate_df


@pytest.fixture
def mock_pandas_df():
    date_range = pd.date_range("2022-11-04", periods=5, freq="D")
    data = {
        "id": [1, 2, 3, 4, 5],
        "letter_code": ["A", "B", "C", "D", "E"],
        "string_date": [
            "2022-11-04",
            "2022-11-05",
            "2022-11-06",
            "2022-11-07",
            "2022-11-08",
        ],
        "date": date_range,
    }
    return pd.DataFrame(data)


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("pytest-spark").getOrCreate()


@pytest.fixture
def mock_spark_df(spark):
    data = [
        (1, "A", "2022-11-04", date(2022, 11, 4)),
        (2, "B", "2022-11-05", date(2022, 11, 5)),
        (3, "C", "2022-11-06", date(2022, 11, 6)),
        (4, "D", "2022-11-07", date(2022, 11, 7)),
        (5, "E", "2022-11-08", date(2022, 11, 8)),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.StringType(), False),
            T.StructField("letter_code", T.StringType(), False),
            T.StructField("string_date", T.StringType(), False),
            T.StructField("date", T.DateType(), False),
        ]
    )
    return spark.createDataFrame(data=data, schema=schema)


@pytest.fixture
def mock_str_date_obj():
    return "2022-11-04"


@pytest.fixture
def mock_datetime_date_obj():
    return date(2022, 11, 4)


@pytest.fixture
def mock_list_date_obj():
    return [
        "2022-11-04",
        date(2022, 11, 5),
        "2022-11-06",
        "2022-11-07",
        date(2022, 11, 8),
    ]


@pytest.fixture
def mock_dict_date_obj():
    return {
        "A": "2022-11-04",
        "B": date(2022, 11, 5),
        "C": "2022-11-06",
        "D": "2022-11-07",
        "E": date(2022, 11, 8),
    }


@pytest.fixture
def desired_values():
    return [
        "November 04, 2022",
        "November 05, 2022",
        "November 06, 2022",
        "November 07, 2022",
        "November 08, 2022",
    ]


@pytest.fixture
def desired_dict_values():
    return {
        "A": "November 04, 2022",
        "B": "November 05, 2022",
        "C": "November 06, 2022",
        "D": "November 07, 2022",
        "E": "November 08, 2022",
    }


# test on panda df
def test_delegate_pandas_df_on_date_column_return_pandas_df(mock_pandas_df):
    new_df = delegate_df(df=mock_pandas_df, date_column="date", new_format="%B %d, %Y")
    assert isinstance(new_df, pandas_data_frame)


def test_delegate_pandas_df_on_date_column_return_correct_value(mock_pandas_df, desired_values):
    new_df = delegate_df(df=mock_pandas_df, date_column="date", new_format="%B %d, %Y")
    check_cond = new_df["date"] == desired_values
    assert check_cond.all()


def test_delegate_pandas_df_with_inappropriate_new_format_datetime_pattern_should_raise_value_error(
    mock_pandas_df,
):
    with pytest.raises(ValueError):
        delegate_df(df=mock_pandas_df, date_column="date", new_format="MMMM dd, yyy")


def test_delegate_pandas_df_on_string_date_column_return_pandas_df(mock_pandas_df):
    new_df = delegate_df(df=mock_pandas_df, date_column="string_date", new_format="%B %d, %Y")
    assert isinstance(new_df, pandas_data_frame)


def test_delegate_pandas_df_on_string_date_column_return_correct_value(mock_pandas_df, desired_values):
    new_df = delegate_df(df=mock_pandas_df, date_column="string_date", new_format="%B %d, %Y")
    check_cond = new_df["string_date"] == desired_values
    assert check_cond.all()


def test_delegate_pandas_df_on_non_date_column_should_raise_value_error(mock_pandas_df):
    with pytest.raises(ValueError):
        delegate_df(df=mock_pandas_df, date_column="letter_code", new_format="%B %d, %Y")


def test_delegate_pandas_df_when_missing_date_column_argument_should_raise_type_error(
    mock_pandas_df,
):
    with pytest.raises(TypeError):
        delegate_df(df=mock_pandas_df, new_format="%B %d, %Y")


# test on spark df
def delegated_spark_df_res_list(data_frame: spark_data_frame, delegated_col: str):
    val = data_frame.select(delegated_col).collect()
    return [e.__getattr__(delegated_col) for e in val]


def test_delegate_spark_df_on_date_column_return_spark_df(mock_spark_df):
    new_df = delegate_df(df=mock_spark_df, date_column="date", new_format="MMMM dd, yyy")
    assert isinstance(new_df, spark_data_frame)


def test_delegate_spark_df_on_date_column_return_correct_value(mock_spark_df, desired_values):
    new_df = delegate_df(df=mock_spark_df, date_column="date", new_format="MMMM dd, yyy")
    delegated_values = delegated_spark_df_res_list(data_frame=new_df, delegated_col="date")
    check_cond = delegated_values == desired_values
    assert check_cond


# def test_delegate_spark_df_with_inappropriate_new_format_pattern_should_raise_value_error\
# (mock_spark_df):
#     with pytest.raises(ValueError):
#         delegate_df(df=mock_spark_df, date_column='date', new_format='%B %d, %Y')


def test_delegate_spark_df_on_string_date_column_return_spark_df(mock_spark_df):
    new_df = delegate_df(df=mock_spark_df, date_column="string_date", new_format="MMMM dd, yyy")
    assert isinstance(new_df, spark_data_frame)


def test_delegate_spark_df_on_string_date_column_return_correct_value(mock_spark_df, desired_values):
    new_df = delegate_df(df=mock_spark_df, date_column="string_date", new_format="MMMM dd, yyy")
    delegated_values = delegated_spark_df_res_list(data_frame=new_df, delegated_col="string_date")
    check_cond = delegated_values == desired_values
    assert check_cond


def test_delegate_spark_df_on_non_date_column_should_raise_value_error(mock_spark_df):
    with pytest.raises(ValueError):
        delegate_df(df=mock_spark_df, date_column="letter_code", new_format="MMMM dd, yyy")


def test_delegate_spark_df_when_missing_date_column_argument_should_raise_type_error(
    mock_spark_df,
):
    with pytest.raises(TypeError):
        delegate_df(df=mock_spark_df, new_format="MMMM dd, yyy")


# test on python object
# str obj
def test_delegate_string_date_obj_return_string_type_value(mock_str_date_obj):
    new_str_obj = delegate_df(df=mock_str_date_obj, new_format="%B %d, %Y")
    assert isinstance(new_str_obj, str)


def test_delegate_string_date_obj_return_correct_value(mock_str_date_obj):
    new_str_obj = delegate_df(df=mock_str_date_obj, new_format="%B %d, %Y")
    assert new_str_obj == "November 04, 2022"


def test_delegate_string_date_obj_with_inappropriate_new_format_pattern_should_raise_value_error(
    mock_str_date_obj,
):
    with pytest.raises(ValueError):
        delegate_df(df=mock_str_date_obj, new_format="MMMM dd, yyy")

    with pytest.raises(ValueError):
        delegate_df(df=mock_str_date_obj, new_format="%abcd")


def test_delegate_inappropriate_string_date_obj_should_raise_value_error():
    with pytest.raises(ValueError):
        delegate_df(df="abc", new_format="%B %d, %Y")


# datetime/date obj
def test_delegate_datetime_date_obj_return_str_type_value(mock_datetime_date_obj):
    new_datetime_obj = delegate_df(df=mock_datetime_date_obj, new_format="%B %d, %Y")
    assert isinstance(new_datetime_obj, str)


def test_delegate_datetime_date_obj_return_correct_value(mock_datetime_date_obj):
    new_datetime_obj = delegate_df(df=mock_datetime_date_obj, new_format="%B %d, %Y")
    assert new_datetime_obj == "November 04, 2022"


def test_delegate_datetime_date_obj_with_inappropriate_new_format_pattern_should_raise_value_error(
    mock_datetime_date_obj,
):
    with pytest.raises(ValueError):
        delegate_df(df=mock_datetime_date_obj, new_format="MMMM dd, yyy")

    with pytest.raises(ValueError):
        delegate_df(df=mock_datetime_date_obj, new_format="%abcd")


# list obj
def test_delegate_list_date_obj_return_list_type_value(mock_list_date_obj):
    new_list_obj = delegate_df(df=mock_list_date_obj, new_format="%B %d, %Y")
    assert isinstance(new_list_obj, list)


def test_delegate_list_date_obj_return_correct_value(mock_list_date_obj, desired_values):
    new_list_obj = delegate_df(df=mock_list_date_obj, new_format="%B %d, %Y")
    assert new_list_obj == desired_values


def test_delegate_list_date_obj_with_inappropriate_new_format_pattern_should_raise_value_error(
    mock_list_date_obj,
):
    with pytest.raises(ValueError):
        delegate_df(df=mock_list_date_obj, new_format="MMMM dd, yyy")

    with pytest.raises(ValueError):
        delegate_df(df=mock_list_date_obj, new_format="%abcd")


# dict obj
def test_delegate_dict_date_obj_return_dict_type_value(mock_dict_date_obj):
    new_dict_obj = delegate_df(df=mock_dict_date_obj, new_format="%B %d, %Y")
    assert isinstance(new_dict_obj, dict)


def test_delegate_dict_date_obj_return_correct_value(mock_dict_date_obj, desired_dict_values):
    new_dict_obj = delegate_df(df=mock_dict_date_obj, new_format="%B %d, %Y")
    assert new_dict_obj == desired_dict_values


def test_delegate_dict_date_obj_with_inappropriate_new_format_pattern_should_raise_value_error(
    mock_dict_date_obj,
):
    with pytest.raises(ValueError):
        delegate_df(df=mock_dict_date_obj, new_format="MMMM dd, yyy")

    with pytest.raises(ValueError):
        delegate_df(df=mock_dict_date_obj, new_format="%abcd")
