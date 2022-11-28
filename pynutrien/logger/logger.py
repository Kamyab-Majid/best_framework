import os
import logging
import traceback
from timeit import default_timer as timer

__all__ = ["Logger", "RuntimeLogger"]

# This will only be set when Glue/Lambda doesn't set root logger, it should only be set
# for EMR/debugging, CloudWatch will add it's own root automatically
default_log_args = {
    "level": logging.DEBUG if os.environ.get("DEBUG", False) else logging.INFO,
    # "level": logging.DEBUG,
    "format": "%(asctime)s.%(msecs)d [%(levelname)s] [%(module)s:%(lineno)d] %(threadName)s:%(name)s - %(message)s",
    "datefmt": "%Y-%m-%d %H:%M:%S",
    # "force": True, # python >=3.8
    # add handler = [ file handler , stream handler]
}


class Logger:
    """A basiclogger. All calls to this function with a given name return the same logger instance.
    This means that logger instances never need to be passed between different parts of an application.
    """

    _last_name = None

    def __new__(cls, name: str = None, **kwargs):
        """making a new instance if the name is None otherwise only make a new one if the name is not found.

        Args:
            name (_type_, optional): the name of the logger. Defaults to None.
            **kwargs: the config key word arguments of the logging.

        """
        # Allow calling Logger('my_job_name')
        # then retrieving the same object with Logger()
        if name is None:
            if cls._last_name is None:
                name = "driver"
            else:
                name = cls._last_name
        cls._last_name = name

        # if glue_context is not None:
        #     return glue_context.get_logger()  # TODO may need wrapper

        # if len(logging.getLogger().handlers) > 0:  # set by glue/lambda
        #     return logging.getLogger().getChild(name)  # TODO .setLevel()? take as arg

        logging.basicConfig(**{**default_log_args, **kwargs})
        logger = logging.getLogger(name)
        # add stream logger and file logger
        # extend with database logging?
        # csv log output to file?

        return logger


class RuntimeLogger:
    """a logger to include the duration of a run with and without an error. In case of an error,
    it will log the time until the error is happened.
    """

    def __init__(self, logger: Logger, name: str, show_traceback: bool = False):
        """configuring the RuntimeLogger.

        Args:
            logger (Logger): the logger to use for the run time.
            name (str): the name of the runtime.
            show_traceback (bool, optional): to either or not show traceback in case of an error. Defaults to False.
        """
        self.logger = logger
        self.name = name
        self.start_time = None
        self.end_time = None
        self.elapsed_time = None
        self.show_traceback = show_traceback

    def start(self):
        """to be used in __enter__. starting the timer."""
        self.start_time = timer()
        self.logger.info(f"Starting: {self.name}")

    def stop(self):
        """to be used in __exit__ in case no error happened."""
        self.logger.info(f"End: {self.name} [{self.elapsed_time:0.3f}s]")

    def error(self, exc_type: type, exc_val: type, exc_tb: traceback):
        """ending the runtime in case of an error. The runtime duration is calculated until the error is happened.

        Args:
            exc_type (type): the type of exception.
            exc_val (type): the value of exception.
            exc_tb (traceback): traceback of exception.
        """
        self.logger.info(f"End: {self.name} [{self.elapsed_time:0.3f}s] with error")
        # TODO prevent this from double printing the traceback when debugging
        if self.show_traceback:
            self.logger.exception(f"Exception Raised: {exc_val!r}")
        else:
            self.logger.error(f"Exception Raised: {exc_val!r}")

    def __enter__(self):
        """the enter dunder method."""
        self.start()
        return self

    def __exit__(self, exc_type: type, exc_val: type, exc_tb: traceback) -> bool:
        """the exit dunder method.

        Args:
            exc_type (type): _description_
            exc_val (type): _description_
            exc_tb (traceback): _description_

        Returns:
            bool: _description_
        """
        self.end_time = timer()
        self.elapsed_time = self.end_time - self.start_time
        if exc_type is None:
            self.stop()
        else:
            self.error(exc_type, exc_val, exc_tb)
        return False


if __name__ == "__main__":
    from time import sleep

    logger = Logger(__name__)
    logger2 = Logger()

    assert logger is logger2

    logger.info("Hello")
    logger.warning("Hello")

    with RuntimeLogger(logger, "time_this"):
        sleep(0.1)

    # This should not be done, since it will print the traceback/error for an exception that is handled
    try:
        with RuntimeLogger(logger, "time_this_2"):
            sleep(0.1)
            raise RuntimeError("oops")
            sleep(0.1)
    except RuntimeError:
        pass

    # when debugging, this will print the traceback once for stderr and once for stdout
    # in the glue environment it will be split into the different output streams
    with RuntimeLogger(logger, "time_this_3"):
        sleep(0.1)
        raise RuntimeError("oops2")
        sleep(0.1)
