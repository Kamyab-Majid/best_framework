from abc import ABC, abstractmethod
from pynutrien.logger import Logger, RuntimeLogger

__all__ = ['ETLBase', 'ETLExtendedBase']


class ETLBase(ABC):

    _function_call_order = ['extract', 'transform', 'load']
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
        'setup',
        'pre_extract', 'extract', 'post_extract',
        'pre_transform', 'transform', 'post_transform',
        'pre_load', 'load', 'post_load',
        'cleanup'
    ]


if __name__ == "__main__":

    class TestETL1(ETLExtendedBase):
        job_name = 'TEST_ETL_1'

        def setup(self): pass

        def extract(self): pass

        def transform(self): pass

        def load(self): pass

    class TestETL2(ETLExtendedBase):
        job_name = 'TEST_ETL_2'

        # def job_name(self): pass

        def extract(self): pass

        def transform(self): raise Exception("Test")

        def load(self): pass

    j1 = TestETL1()
    j1.run()

    j2 = TestETL2()
    j2.run()
