from __future__ import annotations

from datetime import datetime

import pytz

__all__ = ["utcnow", "toisoformat", "fromisoformat"]


def utcnow():
    return datetime.now(pytz.UTC)


def toisoformat(dt):
    return dt.astimezone(pytz.UTC).isoformat()


def fromisoformat(s):
    return datetime.fromisoformat(s).astimezone(pytz.UTC)
