from __future__ import annotations

from pynutrien.core import Logger, RuntimeLogger


def test_logger():
    """Testing to see if the logger is the same if the name is the same."""
    logger = Logger(__name__)
    logger2 = Logger()
    assert logger2 is logger


def test_runtime_logger():
    from time import sleep

    """Testing to see if the runtime logger is working in "with".
    """
    logger = Logger(__name__)
    try:
        with RuntimeLogger(logger, "time_this"):
            sleep(0.01)
        assert True
    except AttributeError:
        assert False


# def test_error_handling():
#     """Testing to see if the error is the same as the test_log.log.
#     """

#     logger = Logger(name='error_handling')

#     with RuntimeLogger(logger, 'time_this_3') as cm:
#         sleep(0.1)
#         logger.info("Hello")
#         logger.warning("Hello")
#         # raise RuntimeError("oops2")
#         sleep(0.1)
#         with open("test/logger.log") as f:
#             lines = f.readlines()
#         with open("test/test_log.log") as f:
#             lines2 = f.readlines()

#         assert lines==lines2
