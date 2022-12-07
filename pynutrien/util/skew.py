from __future__ import annotations

from pyspark.sql import functions as f

__all__ = ["partition_distribution"]


def partition_distribution(df):
    df.cache()
    return df.withColumn("pid", f.spark_partition_id()).groupBy("pid").count().orderBy("pid")
