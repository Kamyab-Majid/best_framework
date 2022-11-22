import sys
import json
from abc import ABCMeta
from abc import abstractmethod
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from pynutrien.etl.base import ETLExtendedBase
from pynutrien.aws.glue.catalog import GlueReader, GlueWriter

__all__ = ["GlueJob", "BasicGlueJob", "GlueSparkContext"]


class GlueSparkContext:
    def __init__(self):
        """initiating glue_context for a glue job"""
        self.spark_context = SparkContext.getOrCreate()
        # self.glue_context = GlueContext.getOrCreate(self.spark_context) ## Not working correctly
        self.glue_context = GlueContext(self.spark_context)
        self.spark_session = self.glue_context.spark_session
        self.job = Job(self.glue_context)


class BasicGlueJob(ETLExtendedBase, GlueSparkContext):
    """an abstract class for an ETL job in glue."""

    # job_name = "TEST"  # REMOVE
    # arguments = []
    @property
    @abstractmethod
    def arguments(self):
        """arguments to be passed to the glue job."""
        raise NotImplementedError

    @property
    def read(self):
        return GlueReader(self.glue_context, redshift_tmp_dir=self.args['RedshiftTempDir'])

    @property
    def write(self):
        return GlueWriter(self.glue_context, redshift_tmp_dir=self.args['RedshiftTempDir'])

    def __init__(self, **kwargs):
        """Initiating GlueSparkContext and ETLExtendedBase."""
        GlueSparkContext.__init__(self)
        ETLExtendedBase.__init__(self, self.job_name, **kwargs)
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
        self.logger.info(f"Supplied Arguments: {sys.argv!r}")
        self.logger.info(f"Parsed Arguments: {self.args!r}")
        self.logger.info(f"Available Modules: {sys.modules!r}")
        self.logger.info(f"Spark Config: {self.spark_context.getConf().getAll()!r}")
        self.job.init(self.job_name, self.args)

    def cleanup(self):
        """commiting the job."""
        self.job.commit()


class GlueJob(BasicGlueJob):
    """A glue job with configuration for environment files and config file."""

    # Should use --files with spark-submit
    # or include extra-files with a file named config.json and env.json
    _config_args = ["env_file_path", "cfg_file_path"]

    def __init__(self, **kwargs):
        # may be implemented as property (cannot use +=)
        self.arguments = self.arguments + self._config_args
        super().__init__(**kwargs)

    # TODO config reader
    @classmethod
    def read_config(cls, path):
        # TODO use s3/config module
        return {}

    # TODO config reader
    @classmethod
    def read_env(cls, path):
        return cls.read_config(path)

    def setup(self):
        """setting up the glue job given the file paths."""
        super().setup()
        env = self.read_env(self.args["env_file_path"])
        self.logger.info(f"Parsed Env: {env!r}")
        config = self.read_config(self.args["cfg_file_path"])
        self.logger.info(f"Parsed Config: {config!r}")
        self.args = {**env, **config, **self.args}


if __name__ == "__main__":

    class MyGlueJob(GlueJob):
        job_name = "TEST_GLUE"
        arguments = ["abc"]

        def extract(self):
            self.read.catalog('database', 'table1').view('table1')
            self.read.catalog('database', 'table2').view('table2')

        def transform(self):
            self.df = self.spark_session.sql("""
                SELECT table1.a, table2.b
                FROM table1 LEFT JOIN table2
                ON table1.c = table2.c
                WHERE table1.a = table1.d
                AND table1.e IS NOT NULL
            """)

        def load(self):
            self.write.catalog('database', 'table3').dataframe(self.df)

    job = MyGlueJob()
    job.run()
