from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable, Union

import pandas as pd
from awsglue.dynamicframe import DynamicFrame
from dateutil import parser
from pandas.core.frame import DataFrame as pandas_data_frame
from pyspark.sql.dataframe import DataFrame as spark_data_frame
from pyspark.sql.functions import col, date_format

__all__ = [
    "delegate_df",
    "format_pandas_datetime",
    "format_spark_datetime",
    "format_dynamicframe_datetime",
    "format_python_datetime",
    "is_date",
]


def delegate_df(df: Any, new_format: str, date_column: str = None, glue_context=None) -> None:
    """
    This func determines the type of the dataframe passed in and delegate the specified
    column/object to a new desired date format

    Args:
        df (Any): the dataframe that is to be delegated. Can be Pandas, Spark, Dynamic
        dataframe or Python object
        new_format (str): date format codes that need to be converted to. Please note
        that it is important to use appropriate datetime format codes/ patterns based
        on the type of dataframe passed in. For example:
            - For Pandas, Python: "%B %d, %Y" (more can be found in this link:
            https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior)
            - For Spark: "MMMM dd, yyy" (more can be found in this link:
            https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)
        date_column (str, optional): name of the date column that needs to have datetime formatted
        (only optional for Python object and Dynamic dataframe). Defaults to None.
        glue_context (optional): The GlueContext class object
        that specifies the context for the run of Dynamic dataframe. Defaults to None.

    Raises:
        TypeError: if 'date_column' argument is missing for Pandas dataframe
        TypeError: if 'date_column' argument is missing for Spark dataframe
        TypeError: if 'date_column' argument is missing for Dynamic dataframe

    Returns:
        Any: appropriate dataframe or object with datetime formatted/converted
    """
    if isinstance(df, pandas_data_frame):
        if date_column is None:
            raise TypeError("'date_column' argument is required to delegate Pandas dataframe")
        return format_pandas_datetime(df, date_column, new_format)
    elif isinstance(df, spark_data_frame):
        if date_column is None:
            raise TypeError("'date_column' argument is required to delegate Spark dataframe")
        return format_spark_datetime(df, date_column, new_format)
    elif isinstance(df, DynamicFrame):
        if date_column is None or glue_context is None:
            raise TypeError(
                'Missing required argument ("date_column" or "glue_context")\
                 to delegate dynamic dataframe'
            )
        return format_dynamicframe_datetime(df, date_column, new_format, glue_context)
    else:
        return format_python_datetime(df, new_format)


def format_pandas_datetime(df: pandas_data_frame, date_column: str, new_format: str) -> pandas_data_frame:
    """
    This func specifically formats the datetime of the values from a specified column
    in a Pandas dataframe to the new date format desired

    Args:
        df (pandas_data_frame): the Pandas dataframe in which date value needs to be formatted
        date_column (str): name of the column from dataframe which has date values to be formatted
        new_format (str): date format codes to be formatted into
        (https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior)

    Raises:
        ValueError: when any value is found to not be able to be interpreted as date/datetime
        ValueError: when the new_format argument does not follow standard date format codes
        (https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior)

    Returns:
        pandas_data_frame: result Pandas dataframe which has the new formatted datetime value
    """
    try:
        df[date_column] = df[date_column].dt.strftime(new_format)
    except AttributeError:
        try:
            df[date_column] = pd.to_datetime(df[date_column], errors="raise")
            df[date_column] = df[date_column].dt.strftime(new_format)
        except ValueError:
            raise ValueError(
                f"Value from column '{date_column}' \
                is not an appropriate datetime type"
            )
    try:
        pd.to_datetime(df[date_column], errors="raise")
    except parser.ParserError:
        raise ValueError(
            f'The new_format argument "{new_format}" \
            passed in does not follow standard date format codes'
        )
    return df


def format_spark_datetime(df: spark_data_frame, date_column: str, new_format: str) -> spark_data_frame:
    """
    This func specifically formats the datetime of the values from a specified column
    in a Spark dataframe to the new date format desired

    Args:
        df (spark_data_frame): the Spark dataframe in which date value needs to be formatted
        date_column (str): name of the column from dataframe which has date values to be formatted
        new_format (str): date format codes to be formatted into
        (https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)

    Raises:
        ValueError: when any value is found to not be able to be interpreted as date/datetime
        ValueError: when the new_format argument passed in does not follow Spark datetime patterns (
    https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)

    Returns:
        spark_data_frame: result Spark dataframe which has the new formatted datetime value
    """

    df = df.withColumn(date_column, date_format(col(date_column), new_format))
    if not df.filter(col(date_column).isNull()).rdd.isEmpty():
        raise ValueError(f"Value from column '{date_column}' is not an appropriate datetime type")
    return df


def format_dynamicframe_datetime(df: DynamicFrame, date_column: str, new_format: str, glue_context) -> DynamicFrame:
    """
    This func specifically formats the datetime of the values from a specified column
    in a Dynamic dataframe to the new date format desired

    Args:
        df (DynamicFrame): the Dynamic dataframe in which date value needs to be formatted
        date_column (str): name of the column from dataframe which has date values to be formatted
        new_format (str): date format codes to be formatted into
        (https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)
        glue_context: The GlueContext class object that specifies the context for the transformation

    Returns:
        DynamicFrame: result Dynamic dataframe which has the new formatted datetime value
    """
    return DynamicFrame.fromDF(format_spark_datetime(df.toDF(), date_column, new_format), glue_context, "df")


def format_python_datetime(object_with_date: Union[Iterable, datetime, str], new_format: str) -> Union[Iterable, str]:
    """
    This func specifically formats the datetime values from a Python datetime object
    to the new date format desired


    Args:
        object_with_date (Union[Iterable, datetime, str]): any type of Python object
        (str, dict, list, etc.) where date value needs to be formatted
        new_format (str): date format codes to be formatted into
        (https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior)

    Raises:
        ValueError: when any value is found to not be able to be interpreted as date/datetime
        ValueError: when the new_format argument does not follow standard date format codes
        (https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior)
        NotImplementedError: when the Python object passed in is not iterable or datetime object

    Returns:
        Union[Iterable, str]: result Python object which has the new formatted datetime value
    """

    if isinstance(object_with_date, str):
        if not is_date(object_with_date):
            raise ValueError(f'The input string "{object_with_date}" cannot be interpreted as date')
        new_object = parser.parse(object_with_date).strftime(new_format)
        if not is_date(new_object):
            raise ValueError(
                f'The new_format argument "{new_format}" \
                does not follow standard date format codes'
            )
        return new_object

    elif isinstance(object_with_date, (datetime, date)):
        object_with_date = object_with_date.strftime(new_format)
        if not is_date(object_with_date):
            raise ValueError(
                f'The new_format argument "{new_format}" \
                does not follow standard date format codes'
            )
        return object_with_date
    elif issubclass(type(object_with_date), Iterable):
        return _iter_format_python_datetime(object_with_date, new_format)
    else:
        raise NotImplementedError


def _iter_format_python_datetime(object_with_date: Iterable, new_format: str) -> Iterable:

    from collections.abc import MutableMapping, MutableSequence, MutableSet

    if issubclass(type(object_with_date), MutableSequence):
        for index, date_object in enumerate(object_with_date):
            object_with_date[index] = format_python_datetime(date_object, new_format)
        return object_with_date

    elif issubclass(type(object_with_date), MutableMapping):
        for key in object_with_date:
            date_object = object_with_date[key]
            object_with_date[key] = format_python_datetime(date_object, new_format)
        return object_with_date

    elif issubclass(type(object_with_date), MutableSet):
        res = []
        for date_object in [obj for obj in object_with_date]:
            object_with_date.discard(date_object)
            res.append(format_python_datetime(date_object, new_format))
        for date_object in res:
            object_with_date.add(date_object)
        return object_with_date

    else:
        return [format_python_datetime(date_object, new_format) for date_object in object_with_date]


def is_date(string: str, fuzzy: bool = False) -> bool:
    """
    This func determines if a string can be interpreted as datetime/date value

    Args:
        string (str): string to check for date
        fuzzy (bool, optional): ignore unknown tokens in string if True. Defaults to False.

    Returns:
        bool: whether True (if string can be interpreted as date)
        or False (if string cannot be interpreted as date)
    """
    try:
        parser.parse(string, fuzzy=fuzzy)
        return True
    except ValueError:
        return False
