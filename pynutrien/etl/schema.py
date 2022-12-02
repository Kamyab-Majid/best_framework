from __future__ import annotations

import pyspark.sql.types as t
from pyspark.pandas.typedef import as_spark_type

# if not available can use t._type_mappings


class Field:
    def __init__(self, spark_type, nullable=False):
        self.spark_type = spark_type
        self.nullable = nullable
        # add distinct


# print(t._type_mappings)


class SchemaMeta(type):
    def __new__(mcls, name, mro, attr):
        print("mcls", mcls)
        print("name", name)
        print("mro", mro)
        print("attrs", attr)

        # create the pyspark struct
        # fields = []
        # new_attr = {}
        # for name, obj in attr.items():
        #     print(name, type(obj), obj)

        new_attr = {k: f for k, f in attr.items() if not isinstance(f, Field)}
        fields = []
        if "__annotations__" in attr:
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

    # class level repr
    def __repr__(cls):
        return cls._struct.simpleString()

    def contains(cls, df):  # or schema?
        """all schema columns/types are in dataframe"""
        return df.schema == cls._struct

    def match(cls, df):  # or schema?
        """same columns/types in any order"""
        df
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

    # def merge(cls, other):
    #     pass
    # def __add__(cls, other: Schema):
    #     pass


# Helper to inherit the metaclass


class Schema(metaclass=SchemaMeta):
    pass


class NameSchema(Schema):
    # id: IntegerType
    # first_name: StringType

    # annotations loaded first, forced ordered dict?

    id: int
    fid: int
    first_name = Field(t.StringType(), nullable=True)
    middle_name = Field(t.StringType(), nullable=True)
    last_name = Field(t.StringType(), nullable=True)


class ETLMetaSchema(Schema):
    def extend(self, df):
        df = super().extend(df)
        return df.withColumn("etl_time", "12:00")


class Schema:
    pass


class FullNameSchema(NameSchema):
    full_name: str


def combine_first_last(df: NameSchema) -> FullNameSchema:
    pass
