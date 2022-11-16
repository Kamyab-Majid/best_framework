import logging
import sys
import os
import datetime

default_log_args = {
    "level": logging.DEBUG if os.environ.get("DEBUG", False) else logging.INFO,
    "format": "%(asctime)s.%(msecs)d [%(levelname)s] [%(module)s:%(lineno)d] %(threadName)s:%(name)s - %(message)s",
    "datefmt": "%Y-%m-%d %H:%M:%S",
    # "force": True, # python >=3.8
    "handlers": [logging.handlers.RotatingFileHandler(f"{datetime.datetime.utcnow().date()}_{__name__}.csv", mode='w',
                                                      maxBytes=5242880, backupCount=999999),
                 logging.StreamHandler(sys.stderr)]
}


class Logger:

    def __new__(cls, name=None, glue_context=None, **kwargs):
        if name is None:
            name = __name__
        if glue_context is not None:
            return glue_context.get_logger(name)  # TODO may need wrapper
        if len(logging.getLogger().handlers) > 0:  # set by glue/lambda
            return logging.getLogger().getChild(name)  # TODO .setLevel()? take as arg

        logging.basicConfig(**{**default_log_args, **kwargs})
        logger = logging.getLogger(name)

        return logger


__all__ = ['Logger']

if __name__ == '__main__':
    logger = Logger()
    logger.info("Hello")
    logger.warning("Hello")
