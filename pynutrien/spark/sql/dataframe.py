from pyspark.sql import DataFrame as _DataFrame


class DataFrame(_DataFrame):
    pass


__all__ = ["DataFrame"]