# to_lowercase().strip().replace(" ", "_"); Vedran to think about strategy to preserve business names in catalog - business names to be preserved in catalog in intial layer (raw) only
from typing import Union

from pandas.core.frame import DataFrame as pandas_data_frame
from pyspark.sql.dataframe import DataFrame as spark_data_frame
from awsglue.dynamicframe import DynamicFrame


def standardize_column_names(
    df: Union[pandas_data_frame, spark_data_frame, DynamicFrame]):
    if isinstance(df, pandas_data_frame):
        df.columns = map(str.lower, df.columns)
        df.columns = map(lambda name: name.replace(" ", "_"))
    elif isinstance(df, spark_data_frame):
        new_col_names = [col_name.lower().replace(" ", "_") for col_name in
                         df.columns]
        return df.toDF(new_col_names)
    elif isinstance(df, DynamicFrame):
        map_ = []
        for index in range(len(df.schema().fields)):
            map_.append((df.schema().fields[index].name,
                         df.schema().fields[index].name.lower().replace(" ",
                                                                        "_")))
        df = df.apply_mapping(map_)
        return df


if __name__ == '__main__':
    pass
