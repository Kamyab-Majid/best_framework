from __future__ import annotations

from pyspark.sql import functions as f
from pyspark.sql import types as t

from pynutrien.util.utctime import fromisoformat, toisoformat

__all__ = ["toisoformat", "fromisoformat"]

toisoformat = f.udf(toisoformat, t.StringType())
fromisoformat = f.udf(fromisoformat, t.TimestampType())
