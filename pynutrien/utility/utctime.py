import time

import pytz
from dateutil.tz import tzlocal
from datetime import datetime

# tzlocal() is the system set timezone
__all__ = ['utcnow', 'toisoformat', 'fromisoformat']

def utcnow():
    return datetime.now(pytz.UTC)

def toisoformat(dt):
    return dt.astimezone(pytz.UTC).isoformat()

def fromisoformat(s):
    return datetime.fromisoformat(s).astimezone(pytz.UTC)

