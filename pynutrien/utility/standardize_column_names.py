from typing import Union

from pandas.core.frame import DataFrame as pandas_data_frame
from pandas.core.indexes.range import RangeIndex
from pyspark.sql.dataframe import DataFrame as spark_data_frame
from awsglue.dynamicframe import DynamicFrame


def standardize_column_names(
    df: Union[pandas_data_frame, spark_data_frame]
    ) -> Union[pandas_data_frame, spark_data_frame]:
    """This method standardizes column names of either pandas, spark or Dynamic dataframe

    Args:
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
            raise RuntimeError(
                "Could not standardize column name due to missing headers in the dataframe.")
        df.columns = df.columns.astype(str)
        df.columns = map(str.strip, df.columns)
        df.columns = map(str.lower, df.columns)
        df.columns = map(lambda name: name.replace(" ", "_"), df.columns)
        return df

    if isinstance(df, spark_data_frame):
        if df.columns:
            if '' in df.columns:
                raise RuntimeError(
                    "Could not standardize column name due to missing headers in the dataframe.")
            new_col_names = [col_name.strip().lower().replace(" ", "_") for col_name in df.columns]
            return df.toDF(*new_col_names)

        raise RuntimeError(
                    "Could not standardize column name due to missing headers in the dataframe.")

    if isinstance(df, DynamicFrame):
        map_ = []
        df_cols = df.schema().fields
        for col in df_cols:
            map_.append(
                (
                    col.name,
                    col.name.strip().lower().replace(" ","_")
                )
            )
        df = df.apply_mapping(map_)
        return df

if __name__ == '__main__':

    # There're 4 scenarios for column names to appear in a dataset
    # 1. Dataset has headers (read from csv as "string" even when it's integer value) -> should work normally
    # 2. Dataset has headers (directly created from pandas so when it's integer, column name is read as "integer")
    #     -> a TypeError will be raised by the system
    # 3. Dataset has no headers (read from csv as "string" ("Unnamed")) -> column names become "Unnamed" and work normally
    # 4. Dataset has no headers (directly created from pandas so an integer will be assigned automatically to name)
    #     -> a TypeError will be raised by the system

    # import pandas as pd
    # df = pd.read_csv("tests/std_column_names_test_files/bigmac.csv")
    # df = pd.DataFrame([[0,423,2342],[12,123,1231],[1,3,5]])
    # df = pd.DataFrame({
    #     0: ['a','b'],
    #     1: ['c','d'],
    #     'price': [2,4]
    # # })
    # print(df)
    # df.columns = df.columns.astype(str)
    # print(type(df.columns))
    # print(df.columns.dtype)
    # print(list(df))
    # print(standardize_column_names(df))
    import pyspark.sql.types as T
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    spark_df_with_integer_headers = spark.createDataFrame(data=[
            ('a','c',2),
            ('b','d',4)
        ],
        schema = T.StructType([
        T.StructField('1         ', T.StringType(), False),
        T.StructField('2', T.StringType(), False),
        T.StructField('Price   ', T.IntegerType(), False),
    ]))

    new_df = standardize_column_names(spark_df_with_integer_headers)
    print(type(new_df))
    new_df.printSchema()
