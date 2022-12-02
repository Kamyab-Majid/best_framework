from __future__ import annotations

import sys
from abc import abstractmethod

import s3fs
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

import pynutrien
from pynutrien.aws.boto import get_boto_session
from pynutrien.aws.glue.catalog import GlueReader, GlueWriter
from pynutrien.aws.s3 import S3Operations
from pynutrien.core.config import ConfigReader
from pynutrien.etl import ETLExtendedBase

__all__ = ["GlueJob", "BasicGlueJob", "GlueSparkContext"]


class GlueSparkContext:
    def __init__(self):
        """initiating glue_context for a glue job"""
        self.boto_session = get_boto_session()
        self.spark_context = SparkContext.getOrCreate()
        self.glue_context = GlueContext(self.spark_context)
        self.spark_session = self.glue_context.spark_session
        self.fs = s3fs.S3FileSystem(anon=False)
        self.s3 = S3Operations(self.boto_session)


class BasicGlueJob(ETLExtendedBase, GlueSparkContext):
    """an abstract class for an ETL job in glue."""

    # job_name = "TEST"
    # arguments = []
    @property
    @abstractmethod
    def arguments(self):
        """arguments to be passed to the glue job."""
        raise NotImplementedError

    @property
    def read(self):
        return GlueReader(self.glue_context, redshift_tmp_dir=self.args["RedshiftTempDir"])

    @property
    def write(self):
        return GlueWriter(self.glue_context, redshift_tmp_dir=self.args["RedshiftTempDir"])

    def __init__(self, **kwargs):
        """Initiating GlueSparkContext and ETLExtendedBase."""
        GlueSparkContext.__init__(self)
        ETLExtendedBase.__init__(self, self.job_name, **kwargs)
        self.job: Job = Job(self.glue_context)
        # self.arg_parser = argparse.ArgumentParser()

    def setup_arguments(self):
        """gettomg environment variables"""
        # glue_args = getResolvedOptions(sys.argv, self.arguments)
        # args, unknown = self.arg_parser.parse_known_args(args=sys.argv)
        # self.args = {**glue_args, **dict(vars(args))}

        # All arguments are mandatory
        self.args = getResolvedOptions(sys.argv, self.arguments)

    def setup(self):
        """logging the arguments and spark config."""
        self.setup_arguments()
        self.logger.info(f"Library Version: {pynutrien.__version__!r}")
        self.logger.info(f"Supplied Arguments: {sys.argv!r}")
        self.logger.info(f"Parsed Arguments: {self.args!r}")
        self.logger.info(f"Spark Config: {self.spark_context.getConf().getAll()!r}")
        self.job.init(self.job_name, self.args)

    def cleanup(self):
        """commiting the job."""
        if self.job is not None and self.job.isInitialized():
            self.job.commit()


class GlueJob(BasicGlueJob):
    """A glue job with configuration for environment files and config file."""

    # Should use --files with spark-submit
    # or include extra-files with a file named config.json and core.json
    _config_args = ["env_file_path", "cfg_file_path"]

    def __init__(self, **kwargs):
        # may be implemented as @property (cannot use +=)
        self.arguments = self.arguments + self._config_args
        super().__init__(**kwargs)

    def read_config(self, path: str) -> dict:
        if path.strip().lower() == "dummy":
            self.logger.info(f"Skipping parsing config file: {path}")
            return {}
        self.logger.info(f"Reading config: {path}")
        return ConfigReader(path).read()

    def read_env(self, path: str) -> dict:
        return self.read_config(path)

    def setup(self):
        """setting up the glue job given the file paths."""
        super().setup()
        env = self.read_env(self.args["env_file_path"])
        self.logger.info(f"Parsed Env: {env!r}")
        config = self.read_config(self.args["cfg_file_path"])
        self.logger.info(f"Parsed Config: {config!r}")
        self.args = {**env, **config, **self.args}
