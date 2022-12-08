from __future__ import annotations

import pyspark.sql.types as t


class Field:
    def __init__(self, spark_type, nullable=False):
        self.spark_type = spark_type
        self.nullable = nullable


class SchemaMeta(type):
    def __new__(mcls, name, mro, attr):
        new_attr = {k: f for k, f in attr.items() if not isinstance(f, Field)}
        fields = []
        if "__annotations__" in attr:

            # TODO solve dependency
            from pyspark.pandas.typedef import as_spark_type
            # if not available can use t._type_mappings
            # print(t._type_mappings)

            fields.extend(
            [t.StructField(name, as_spark_type(pyt), False) for name, pyt in attr["__annotations__"].items()]
            )
            del new_attr["__annotations__"]
        fields.extend(
            [t.StructField(name, f.spark_type, f.nullable) for name, f in attr.items() if isinstance(f, Field)]
        )

        class_obj = super().__new__(mcls, name, mro, new_attr)
        class_obj._struct = t.StructType(fields)
        return class_obj

    def __repr__(cls):
        return cls._struct.simpleString()

    def contains(cls, df):  # or schema?
        """all schema columns/types are in dataframe"""
        return df.schema == cls._struct

    def match(cls, df):  # or schema?
        """same columns/types in any order"""
        pass

    def exact(cls, df):  # or schema?
        """same columns/types in order"""
        return df.schema == cls._struct

    def cast(cls, df):
        """force dataframe to cast to type"""
        pass

    def impose(cls, df):
        """force dataframe to cast to type and select only intersected columns"""
        pass

    def extend(cls, df):
        """force dataframe to cast to type and add extra columns"""
        pass


class Schema(metaclass=SchemaMeta):
    pass


if __name__ == '__main__':

    class NameSchema(Schema):
        # id: IntegerType
        # first_name: StringType
        id: int
        fid: int
        first_name = Field(t.StringType(), nullable=True)
        middle_name = Field(t.StringType(), nullable=True)
        last_name = Field(t.StringType(), nullable=True)

        def extend(self, df):
            df = super().extend(df)
            return df.withColumn("etl_time", "12:00")


