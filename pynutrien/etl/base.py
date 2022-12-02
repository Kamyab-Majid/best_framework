from __future__ import annotations

from abc import ABC, abstractmethod

from pynutrien.core import Logger, RuntimeLogger

__all__ = ["ETLBase", "ETLExtendedBase"]


class ETLBase(ABC):

    _function_call_order = ["extract", "transform", "load"]
    _current_state = None

    def __init__(self, *args, **kwargs):
        self.logger = Logger(*args, **kwargs)

    @property
    @abstractmethod
    def job_name(self):
        raise NotImplementedError

    @abstractmethod
    def extract(self):
        raise NotImplementedError

    @abstractmethod
    def transform(self):
        raise NotImplementedError

    @abstractmethod
    def load(self):
        raise NotImplementedError

    def run(self):
        with RuntimeLogger(self.logger, self.job_name, show_traceback=True):
            for fname in self._function_call_order:
                self._current_state = fname
                if hasattr(self, fname):
                    fn = getattr(self, fname)
                    with RuntimeLogger(self.logger, fname):
                        fn()


class ETLExtendedBase(ETLBase):
    _function_call_order = [
        "setup",
        "pre_extract",
        "extract",
        "post_extract",
        "pre_transform",
        "transform",
        "post_transform",
        "pre_load",
        "load",
        "post_load",
        "cleanup",
    ]
