from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from pynutrien.util.utctime import utcnow

__all__ = ["add_etl_audit_columns"]


def _pandas_add_etl_audit_columns(df):
    df["etl_time"] = utcnow()
    return df


def _spark_add_etl_audit_columns(df):
    return df.withColumn("etl_time", f.lit(utcnow()))


def add_etl_audit_columns(df):
    if isinstance(df, pd.DataFrame):
        return _pandas_add_etl_audit_columns(df)

    if isinstance(df, DataFrame):
        return _spark_add_etl_audit_columns(df)

    raise TypeError(f"Unknown dataframe type: {type(df)!r}")
