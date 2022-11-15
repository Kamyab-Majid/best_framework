import logging
from typing import Any

import boto3

import pandas as pd
import pyspark.sql
import s3fs  # pre-requisite for pandas to read from s3

boto3.set_stream_logger('boto3.resources', logging.INFO)


class S3Operations:
    def __init__(self, session):
        self.session: boto3.session.Session = session
        self.s3_resource: boto3.resources.factory.s3_resource.ServiceResource = self.session.resource(
            "s3")
        self.s3_client = self.session.client("s3")
        self.s3_exceptions = (
            self.s3_resource.meta.client.exceptions.BucketAlreadyExists,
            self.s3_resource.meta.client.exceptions.BucketAlreadyOwnedByYou,
            self.s3_resource.meta.client.exceptions.ClientError,
            self.s3_resource.meta.client.exceptions.InvalidObjectState,
            self.s3_resource.meta.client.exceptions.NoSuchBucket,
            self.s3_resource.meta.client.exceptions.NoSuchKey,
            self.s3_resource.meta.client.exceptions.NoSuchUpload,
            self.s3_resource.meta.client.exceptions.ObjectAlreadyInActiveTierError,
            self.s3_resource.meta.client.exceptions.ObjectNotInActiveTierError)

    def list_buckets(self):
        try:
            return list(self.s3_resource.buckets.all())
        except self.s3_exceptions:
            logging.exception("Could not list buckets due to client error")

    def list_all_objects(self, bucket):
        try:
            return self.s3_resource.Bucket(bucket).objects.all()
        except self.s3_exceptions:
            logging.exception(
                f"Could not list objects for {bucket} bucket.")

    def read_object(self, s3_object_path: Any, df_type: str, file_type: str,
                    spark: pyspark.sql.SparkSession = None, glueContext=None,
                    **kwargs):
        pandas_ = {"csv": pd.read_csv, "json": pd.read_json,
                   "parquet": pd.read_parquet, "orc": pd.read_orc,
                   "xml": pd.read_xml, "fwf": pd.read_fwf, "sas": pd.read_sas,
                   "spss": pd.read_spss, "html": pd.read_html,
                   "stata": pd.read_stata, "feather": pd.read_feather,
                   "pickle": pd.read_pickle}

        spark_ = {"parquet": spark.read.parquet, "orc": spark.read.orc,
                  "json": spark.read.json, "csv": spark.read.csv,
                  "text": spark.read.text, "avro": spark.read.avro}

        if df_type == "pandas":
            return pandas_[file_type](s3_object_path, **kwargs)
        elif df_type == "spark":
            return spark_[file_type](s3_object_path, **kwargs)
        elif df_type == "glue":
            return glueContext.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={"paths": ["s3://s3path"]},
                format=f"{file_type}",
                format_options={
                    **kwargs
                },
            )

    def put_object(self, data, bucket, key):
        try:
            object_ = self.s3_resource.Object(bucket, key)
            object_.put(Body=data)
        except self.s3_exceptions:
            logging.exception(
                f"Could not write data to {key} file in {bucket} bucket.")

    def validate_object(self, bucket, key):
        try:
            # Check if the object exists in target via GET request:
            self.s3_client.head_object(Bucket=bucket, Key=key)
        except self.s3_exceptions:
            logging.exception(
                f"Could not validate file {key} in {bucket} bucket.")

    def write_to_s3_from_local(self, bucket, file_path, s3_key,
                               extra_args: dict = None, callback=None,
                               config=None):
        try:
            self.s3_resource.Bucket(bucket).upload_file(file_path, s3_key, extra_args,
                                                        callback, config)
        except self.s3_exceptions:
            logging.exception(
                f"Could not write {file_path} to {bucket} bucket.")

    def move_object(self, source: dict, destination_bucket: str,
                    destination_key: str):
        """

        :param source: A dictionary contains {'Bucket', 'Key', 'VersionId'} keys and values. 'VersionId' is optional
        :param destination_bucket: Name of the destination bucket
        :param destination_key: Name of the destination key
        """
        logging.info(
            "Move is not directly supported by boto3, we are performing copy and delete instead.")
        self.copy_object(source, destination_bucket, destination_key)
        self.s3_resource.Object(source["Bucket"], source["Key"]).delete()

    def copy_object(self, copy_source: dict, destination_bucket: str,
                    destination_key: str):
        """
        :param copy_source: A dictionary contains {'Bucket', 'Key', 'VersionId'} keys and values. 'VersionId' is optional
        :param destination_bucket: Name of the destination bucket
        :param destination_key: Name of the destination key
        """
        try:
            self.s3_resource.meta.client.copy(copy_source, destination_bucket,
                                              destination_key)
            self.validate_object(destination_bucket, destination_key)
        except self.s3_exceptions:
            logging.exception(
                f"Could not copy {copy_source} to {destination_bucket} bucket for {destination_key} key.")

    def delete_object(self, bucket: str, key: str):
        try:
            self.s3_resource.Object(bucket, key).delete()
        except self.s3_exceptions:
            logging.exception(
                f"Could not delete object {key} from {bucket} bucket.")

    def delete_all_objects(self, bucket):
        try:
            objects = self.list_all_objects(bucket)
            for object_ in objects:
                self.delete_object(bucket, object_)
        except self.s3_exceptions:
            logging.exception(f"Could not delete all the objects in {bucket}")

    def move_all_objects(self, bucket, destination_bucket: str,
                         destination_key: str):
        try:
            objects = self.list_all_objects(bucket)
            source = {"Bucket": bucket}
            for object_ in objects:
                source["Key"] = object_
                self.move_object(source, destination_bucket,
                                 destination_key)
        except self.s3_exceptions:
            logging.exception(
                f"Failed to move objects from {source['Bucket']} to {destination_bucket}")

    def copy_all_objects(self, bucket, destination_bucket: str,
                         destination_key: str):
        try:
            objects = self.list_all_objects(bucket)
            source = {"Bucket": bucket}
            for object_ in objects:
                source["Key"] = object_
                self.copy_object(source, destination_bucket, destination_key)
        except self.s3_exceptions:
            logging.exception(
                f"Failed to copy objects from {source['Bucket']} to {destination_bucket}")

    def get_object_tag(self, bucket, key, **kwargs):
        try:
            return self.s3_client.get_object_tagging(Bucket=bucket, Key=key,
                                                     **kwargs)
        except self.s3_exceptions:
            logging.exception(
                f"Failed to get tag for {key} object in {bucket} bucket")

    def add_object_tag(self, bucket, key, new_tag: dict, **kwargs):
        response = self.get_object_tag(bucket, key, **kwargs)
        response["TagSet"].append(new_tag)  # Python 3.5 >
        self.set_object_tag(bucket, key, response["TagSet"])

    def set_object_tag(self, bucket, key, tag_set: list):
        self.s3_client.put_object_tagging(Bucket=bucket, Key=key,
                                          Tagging={"TagSet": tag_set})

    def delete_object_tag(self, bucket, key, tag: dict, **kwargs):
        response = self.get_object_tag(bucket, key, **kwargs)
        return_, new_tag_set = list(), list()
        for dict_ in response["TagSet"]:
            if dict_["Key"] != tag["Key"]:
                new_tag_set.append(dict_)
            else:
                continue

        self.set_object_tag(bucket, key, new_tag_set)

    def set_bucket_tag(self, bucket, **kwargs):
        return self.s3_client.put_bucket_tagging(bucket, **kwargs)


if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read(".env")
    session = boto3.Session(
        aws_access_key_id=config["aws"]["aws_access_key_id"],
        aws_secret_access_key=config["aws"]["aws_secret_access_key"])

    bucket_name = "insights-framework-test"

    s3_operations = S3Operations(session)

    objects = s3_operations.list_all_objects(bucket_name)
    for object_ in objects:
        print(object_.key)

    print(len(s3_operations.list_buckets()))

    copy_source = {"Bucket": bucket_name, "Key": "s3-subfolder/text.txt"}
    s3_operations.copy_object(copy_source, bucket_name, "text.txt")

    s3_operations.add_object_tag(bucket_name, "text.txt",
                                 new_tag={"Key": "New_Key3",
                                          "Value": "Value3"})

    s3_operations.delete_object_tag(bucket_name, "text.txt",
                                    {"Key": "New_Key3",
                                     "Value": "Value3"})
