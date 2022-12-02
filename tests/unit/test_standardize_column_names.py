from __future__ import annotations

import pandas as pd
import pyspark.sql.types as T
import pytest
from pyspark.sql import SparkSession

from pynutrien.util.column import standardize_column_names


@pytest.fixture
def valid_pandas_df():
    return pd.read_csv("tests/test_files/std_column_names_test_files/bigmac.csv")


@pytest.fixture
def expected_column_names():
    return ["date", "country", "price_in_us_dollars"]


@pytest.fixture
def pandas_df_with_no_headers():
    return pd.DataFrame([[0, 423, 2342], [12, 123, 1231], [1, 3, 5]])


@pytest.fixture
def pandas_df_with_integer_headers():
    return pd.DataFrame({1: ["a", "b"], 2: ["c", "d"], "Price  ": [2, 4]})


@pytest.fixture
def spark():
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def valid_spark_df(spark):
    return (
        spark.read.format("csv").option("header", True).load("tests/test_files/std_column_names_test_files/bigmac.csv")
    )


@pytest.fixture
def spark_df_with_no_headers(spark):
    return spark.createDataFrame(
        data=[("a", "c", 2), ("b", "d", 4)],
        schema=T.StructType(
            [
                T.StructField("", T.StringType(), False),
                T.StructField("", T.StringType(), False),
                T.StructField("", T.IntegerType(), False),
            ]
        ),
    )


@pytest.fixture
def spark_df_with_integer_headers(spark):
    data = [("a", "c", 2), ("b", "d", 4)]
    schema = T.StructType(
        [
            T.StructField("1         ", T.StringType(), False),
            T.StructField("2", T.StringType(), False),
            T.StructField("Price   ", T.IntegerType(), False),
        ]
    )
    return spark.createDataFrame(data=data, schema=schema)


# test on pandas df


def test_valid_pandas_df_should_standardize_column_names_as_expected(valid_pandas_df, expected_column_names):
    standardized_column_names = list(standardize_column_names(valid_pandas_df))
    assert standardized_column_names == expected_column_names


def test_pandas_df_with_no_header_should_raise_runtime_error(pandas_df_with_no_headers):
    with pytest.raises(RuntimeError):
        standardize_column_names(pandas_df_with_no_headers)


def test_pandas_df_with_integer_headers_should_standardize_col_name_correctly(pandas_df_with_integer_headers):
    new_df = standardize_column_names(pandas_df_with_integer_headers)
    new_col_names = list(new_df)
    expected_col_names = ["1", "2", "price"]
    assert new_col_names == expected_col_names


# test on spark df


def test_spark_df_column_names_standatdization_valid_csv(valid_spark_df, expected_column_names):
    new_df = standardize_column_names(valid_spark_df)
    standardized_column_names = new_df.columns
    assert standardized_column_names == expected_column_names


def test_spark_df_with_no_header_should_raise_runtime_error(spark_df_with_no_headers):
    with pytest.raises(RuntimeError):
        standardize_column_names(spark_df_with_no_headers)


def test_spark_df_with_integer_headers_should_standardize_col_name_correctly(spark_df_with_integer_headers):
    new_df = standardize_column_names(spark_df_with_integer_headers)
    new_col_names = new_df.columns
    expected_col_names = ["1", "2", "price"]
    assert new_col_names == expected_col_names
