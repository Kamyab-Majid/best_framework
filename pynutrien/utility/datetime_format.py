# pylint: disable=missing-module-docstring, invalid-name, import-error, raise-missing-from, import-outside-toplevel, no-else-return, too-many-branches
from typing import Union, Iterable, Any
from datetime import datetime, date
from dateutil import parser

from pandas.core.frame import DataFrame as pandas_data_frame
from pyspark.sql.dataframe import DataFrame as spark_data_frame
from pyspark.sql.functions import date_format, col
from awsglue.dynamicframe import DynamicFrame

import pandas as pd


def delegate_df(
    df: Any, new_format: str, date_column: str = None, glue_context=None
) -> None:
    """
    This function determines the type of the dataframe passed in and delegate the specified
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
            raise TypeError(
                "'date_column' argument is required to delegate Pandas dataframe"
            )
        return format_pandas_datetime(df, date_column, new_format)
    elif isinstance(df, spark_data_frame):
        if date_column is None:
            raise TypeError(
                "'date_column' argument is required to delegate Spark dataframe"
            )
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


def format_pandas_datetime(
    df: pandas_data_frame, date_column: str, new_format: str
) -> pandas_data_frame:
    """
    This function specifically formats the datetime of the values from a specified column
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


def format_spark_datetime(
    df: spark_data_frame, date_column: str, new_format: str
) -> spark_data_frame:
    """
    This function specifically formats the datetime of the values from a specified column
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
        raise ValueError(
            f"Value from column '{date_column}' is not an appropriate datetime type"
        )
    return df


def format_dynamicframe_datetime(
    df: DynamicFrame, date_column: str, new_format: str, glue_context
) -> DynamicFrame:
    """
    This function specifically formats the datetime of the values from a specified column
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
    return DynamicFrame.fromDF(
        format_spark_datetime(df.toDF(), date_column, new_format), glue_context, "df"
    )


def format_python_datetime(
    object_with_date: Union[Iterable, datetime, str], new_format: str
) -> Union[Iterable, str]:
    """
    This function specifically formats the datetime values from a Python datetime object
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
    from collections.abc import MutableSequence, MutableSet, MutableMapping

    if isinstance(object_with_date, str):
        if not is_date(object_with_date):
            raise ValueError(
                f'The input string "{object_with_date}" cannot be interpreted as date'
            )
        new_object = parser.parse(object_with_date).strftime(new_format)
        if not is_date(new_object):
            raise ValueError(
                f'The new_format argument "{new_format}" \
                does not follow standard date format codes'
            )
        return new_object

    if isinstance(object_with_date, (datetime, date)):
        object_with_date = object_with_date.strftime(new_format)
        if not is_date(object_with_date):
            raise ValueError(
                f'The new_format argument "{new_format}" \
                does not follow standard date format codes'
            )
        return object_with_date

    try:
        iter(object_with_date)
        if issubclass(
            type(object_with_date), (MutableSequence, MutableSet, MutableMapping)
        ):
            if isinstance(object_with_date, dict):
                for key, value in object_with_date.items():
                    object_with_date[key] = format_python_datetime(value, new_format)
            elif isinstance(object_with_date, list):
                for index, date_object in enumerate(object_with_date):
                    object_with_date[index] = format_python_datetime(
                        date_object, new_format
                    )
            else:
                raise NotImplementedError
        else:
            raise NotImplementedError
    except TypeError:
        raise NotImplementedError
    return object_with_date


def is_date(string: str, fuzzy: bool = False) -> bool:
    """
    This function determines if a string can be interpreted as datetime/date value

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


# if __name__ == '__main__':
# date_range = pd.date_range('2022-11-04', periods=5, freq='D')
# data = {
#         'id': [1, 2, 3, 4, 5],
#         'letter_code': ['A','B','C','D','E'],
#         'string_date': ['2022-11-04','2022-11-05','2022-11-06','2022-11-07','NaN'],
#         'date': date_range
#         }
# df = pd.DataFrame(data)
# print(df)

# df['date'] = df['date'].dt.strftime('%B %d, %Y')
# new_df = delegate_df(df=df, date_column='date', new_format='%B %d, %dxfcsz')
# print(new_df)
# from pyspark.sql import SparkSession
# import pyspark.sql.types as T
# from datetime import date
# spark = SparkSession.builder.appName('pytest-spark').getOrCreate()
# data = [
#     (1,'A','2022-11-04',date(2022,11,4)),
#     (2,'B','2022-11-05',date(2022,11,5)),
#     (3,'C','2022-11-06',date(2022,11,6)),
#     (4,'D','2022-11-07',date(2022,11,7)),
#     (5,'E','2022-11-08',date(2022,11,8)),
#     ]
# schema = T.StructType([
#     T.StructField('id', T.StringType(), False),
#     T.StructField('letter_code', T.StringType(), False),
#     T.StructField('string_date', T.StringType(), False),
#     T.StructField('date', T.DateType(), False)
# ])
# df = spark.createDataFrame(data=data, schema=schema)
# df.show()
# date_df = df.select("date").collect()
# date_values = [e.__getattr__("date").strftime('%B %d, %Y') for e in date_df]
# print(date_values)
# df = spark.createDataFrame([('2015-04-08',)], ['dt'])
# new = df.withColumn('date',date_format('dt', 'MM/dd/yyy'))
# new.show()
# new_df = delegate_df(df=df, date_column='string_date',new_format='%B %d, %dxfcsz')
# str_date_col = new_df.select("string_date").collect()
# delated_date_str_list = [e.__getattr__('string_date') for e in str_date_col]
# new_df.show()
# print(delated_date_str_list)
# des = [
#     'November 04, 2022',
#     'November 05, 2022',
#     'November 06, 2022',
#     'November 07, 2022',
#     'November 08, 2022']
# print(delated_date_str_list == des)

# obj_w_date = 'abc'
# print(type(date(2022,11,4)))
# delegated_obj_w_date = delegate_df(df=obj_w_date, new_format='%B %d, %Y')
# print(delegated_obj_w_date)
