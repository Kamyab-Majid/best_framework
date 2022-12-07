from __future__ import annotations


def view(df, table_name):
    df.createOrReplaceTempView(table_name)
