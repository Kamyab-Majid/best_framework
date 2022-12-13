from __future__ import annotations

import sys

from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pynutrien.aws.glue.job import GlueJob

# @pytest.fixture(scope="module", autouse=True)
# def glue_context():
#     sys.argv.append('--JOB_NAME')
#     sys.argv.append('test_count')

#     args = getResolvedOptions(sys.argv, ['JOB_NAME'])
#     context = GlueContext(SparkContext.getOrCreate())
#     job = Job(context)
#     job.init(args['JOB_NAME'], args)

#     yield(context)

#     job.commit()
    

    

def test_glue_class():
    class MyGlueJob(GlueJob):
        job_name = "TEST_GLUE"
        arguments = ["abc"]

        def __init__(self, **kwargs):
            super().__init__(**kwargs)

            if "--JOB_NAME" in sys.argv:
                self.arguments.append("JOB_NAME")
            sys.argv.extend(
                [
                    "--cfg_file_path",
                    "./tests/test_files/config_files/config.json",
                    "--env_file_path",
                    "./tests/test_files/config_files/config.json",
                    "--abc",
                    "123",
                ]
            )
            args = getResolvedOptions(sys.argv, self.arguments)

            self.job = Job(self.glue_context)

            if "JOB_NAME" in args:
                jobname = args["JOB_NAME"]
            else:
                jobname = "test"
            self.job.init(jobname, args)

        def extract(self, path="s3://awsglue-datasets/examples/us-legislators/all/persons.json"):
            self.dynamicframe = self.glue_context.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={"paths": [path], "recurse": True},
                format="json",
            )
            self.job.commit()

        def transform(
            self,
        ):
            pass

        def load(
            self,
        ):
            pass

    job = MyGlueJob()
    job.run()
    assert job.dynamicframe.toDF().count() == 1961
