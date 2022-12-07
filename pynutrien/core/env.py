from __future__ import annotations

import os


class Environment:

    NAME_SPACE = "insights"
    SEPARATOR = "_"

    @classmethod
    def set_env(self, region=None, env=None, project=None):
        os.environ["AWS_REGION"] = region or os.environ["AWS_REGION"]
        os.environ["AWS_ENV_NAME"] = env or os.environ["AWS_ENV_NAME"]
        os.environ["PROJECT_NAME"] = project or os.environ["PROJECT_NAME"]
        return os.environ["AWS_REGION"], os.environ["AWS_ENV_NAME"], os.environ["PROJECT_NAME"]

    @classmethod
    def get_env(self):
        return os.environ["AWS_REGION"], os.environ["AWS_ENV_NAME"], os.environ["PROJECT_NAME"]

    def __init__(self, region=None, env=None, project=None):
        self.region, self.env, self.project = self.set_env(region, env, project)

    def get_name(self, obj_name):
        return self.SEPARATOR.join([self.NAME_SPACE, self.region, self.env, self.project, obj_name])

    def get_generic_name(self, obj_name):
        return self.SEPARATOR.join([self.NAME_SPACE, self.region, self.env, obj_name])
