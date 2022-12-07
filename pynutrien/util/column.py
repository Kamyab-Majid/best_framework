from __future__ import annotations

import re
import unicodedata
from typing import Union

from awsglue.dynamicframe import DynamicFrame
from pandas.core.frame import DataFrame as pandas_data_frame
from pandas.core.indexes.range import RangeIndex
from pyspark.sql.dataframe import DataFrame as spark_data_frame


def standardize_column_name(name: str) -> str:
    formatted_name = str(name).strip().lower().replace(" ", "_").replace("-", "_")
    # Removes all non-alphanumeric characters (and _)
    return re.sub(r"\W", "", formatted_name)


def normalize_column_name(name: str) -> str:
    std_name = standardize_column_name(name)
    return "".join(c for c in unicodedata.normalize("NFD", std_name) if ord(c) <= 0x7F)


def apply_column_function(
    func: callable, df: Union[pandas_data_frame, spark_data_frame]
) -> Union[pandas_data_frame, spark_data_frame]:
    """This method standardizes column names of either pandas, spark or Dynamic dataframe

    Args:
        func callable
        df (Union[pandas_data_frame, spark_data_frame]):
        pandas, spark or DynamicFrame df where column names need to be standardized

    Raises:
        RuntimeError: when dataframe is missing headers/column names

    Returns:
        Union[pandas_data_frame, spark_data_frame]:
        a new dataframe which has column names already standardized
    """
    if isinstance(df, pandas_data_frame):
        if isinstance(df.columns, RangeIndex):
            raise RuntimeError("Could not standardize column name due to missing headers in the dataframe.")
        df.columns = map(func, df.columns.astype(str))
        return df

    elif isinstance(df, spark_data_frame):
        if df.columns:
            if "" in df.columns:
                raise RuntimeError("Could not standardize column name due to missing headers in the dataframe.")
            # new_col_names = [col_name.strip().lower().replace(" ", "_") for col_name in df.columns]
            # new_col_names = [normalize_column_name(col_name) for col_name in df.columns]
            return df.toDF(*map(func, df.columns))

        raise RuntimeError("Could not standardize column name due to missing headers in the dataframe.")

    elif isinstance(df, DynamicFrame):
        map_ = []
        df_cols = df.schema().fields
        for col in df_cols:
            map_.append((col.name, func(col.name)))
        df = df.apply_mapping(map_)
        return df

    else:
        raise TypeError(f"Unknown dataframe type: {type(df)}")


def standardize_column_names(df: pandas_data_frame | spark_data_frame) -> pandas_data_frame | spark_data_frame:
    """Alias for apply_column_function(standardize_column_name, df)"""
    return apply_column_function(standardize_column_name, df)


def normalize_column_names(df: pandas_data_frame | spark_data_frame) -> pandas_data_frame | spark_data_frame:
    """Alias for apply_column_function(normalize_column_name, df)"""
    return apply_column_function(normalize_column_name, df)
