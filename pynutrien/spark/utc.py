
from pynutrien.utility.utctime import fromisoformat, toisoformat, utcnow

from pyspark.sql import types as t
from pyspark.sql import functions as f

__all__ = ['toisoformat', 'fromisoformat']

toisoformat = f.udf(toisoformat, t.StringType())
fromisoformat = f.udf(fromisoformat, t.TimestampType())
