from __future__ import annotations
from typing import Union, Iterable, Any
from dateutil import parser
from datetime import datetime

from pandas.core.frame import DataFrame as pandas_data_frame
from pyspark.sql.dataframe import DataFrame as spark_data_frame
from pyspark.sql.functions import date_format, col
# from awsglue.dynamicframe import DynamicFrame

import pandas as pd

def delegate_df(df: Any, date_column: str, new_format: str,
                glue_context=None) -> None:
    if isinstance(df, pandas_data_frame):
        return format_pandas_datetime(df, date_column, new_format)
    elif isinstance(df, spark_data_frame):
        return format_spark_datetime(df, date_column, new_format)
    elif isinstance(df, DynamicFrame):
        return format_dynamicframe_datetime(df, date_column, new_format, glue_context)
    else:
        return format_python_datetime(df, new_format)


def format_pandas_datetime(df: pandas_data_frame, date_column: str,
                           new_format: str) -> pandas_data_frame:
    try:
        df[date_column] = df[date_column].dt.strftime(new_format)
    except AttributeError:
        df[date_column] = pd.to_datetime(df[date_column], errors='raise')
        df[date_column] = df[date_column].dt.strftime(new_format)
    except ValueError:
        raise ValueError(f"Value from column {date_column} is not an appropriate datetime type")
    return df


def format_spark_datetime(df: spark_data_frame, date_column: str,
                          new_format: str) -> spark_data_frame:
    return df.withColumn(date_column,
                         date_format(col(date_column), new_format))


def format_dynamicframe_datetime(df: DynamicFrame, date_column: str,
                                 new_format: str,
                                 glue_context) -> DynamicFrame:
    return DynamicFrame.fromDF(
        format_spark_datetime(df.toDF(), date_column, new_format),
        glue_context, "df")


def format_python_datetime(object_with_date: Union[Iterable, datetime, str],
                           new_format: str) -> Union[Iterable, datetime]:
    from collections.abc import MutableSequence, MutableSet, MutableMapping
    try:
        iter(object_with_date)
        if isinstance(object_with_date, str):
            new_object = parser.parse(object_with_date)
            return new_object.strftime(new_format)

        elif issubclass(type(object_with_date),
                        (MutableSequence, MutableSet, MutableMapping)):
            if isinstance(type(object_with_date), dict):
                for key, value in object_with_date.items():
                    object_with_date[key] = value.strftime(new_format)
            elif isinstance(type(object_with_date), list):
                for index, date_object in object_with_date:
                    object_with_date[index] = date_object.strftime(new_format)
            else:
                raise NotImplementedError
        else:
            raise NotImplementedError

    except TypeError:
        if isinstance(object_with_date, datetime):
            object_with_date = object_with_date.strftime(new_format)
            return object_with_date
        else:
            raise NotImplementedError

    return object_with_date


if __name__ == '__main__':
    date_range = pd.date_range('2022-11-04', periods=5, freq='D')
    data = {
            'id': [1, 2, 3, 4, 5],
            'letter_code': ['A','B','C','D','E'],
            'string_date': ['2022-11-04','2022-11-05','2022-11-06','2022-11-07','NaN'],
            'date': date_range
            }
    df = pd.DataFrame(data)
    print(df)

    # df['date'] = df['date'].dt.strftime('%B %d, %Y')
    new_df = delegate_df(df=df, date_column='string_date', new_format='%B %d, %Y')
    print(new_df['string_date'][0] == 'November 04, 2022')
    df['date'] = pd.to_datetime(df['date'], format='%B %d, %Y', errors='raise')
    print(df)
