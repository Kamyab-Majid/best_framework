from __future__ import annotations

import logging
import typing
from typing import Any

import boto3
from botocore.exceptions import ClientError, ParamValidationError

from pynutrien.aws.boto import get_boto_session

boto3.set_stream_logger("boto3.resources", logging.INFO)

if typing.TYPE_CHECKING:
    import pyspark


class S3Operations:
    """
    This class acts as a S3 module to let user perform certain operations on Amazon S3 services
    including but not limited to:
    - List all buckets currently in S3
    - List all objects in a specific bucket
    - Read an object
    - Put an object
    - Write an object from local
    - Validate an object to see if it exists
    - Copy/Move/Delete an object or all objects
    """

    def __init__(self, session=None) -> None:
        """
        A boto3 session is created once this class initializes taking the AWS access key and AWS
        secret access key (stored in a dotenv(.core) file)
        Args:
            session: a boto3 session which takes in AWS access key ID and AWS secret access key ID
            to get access to an AWS account
        """
        self.session: boto3.session.Session = session or get_boto_session()
        self.s3_resource: boto3.resources.factory.s3_resource.ServiceResource = self.session.resource("s3")
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
            self.s3_resource.meta.client.exceptions.ObjectNotInActiveTierError,
        )

    def list_buckets(self) -> list:
        """
        This method lists all the buckets available on Nutrien's S3 platform.

        Raises:
            RuntimeError: If the valid credentials are not entered
            or if the role is not given the right permisisons.

        Returns:
            list: The list of all buckets name
        """
        try:
            return list(self.s3_resource.buckets.all())
        except ClientError as exc:
            raise RuntimeError("Could not list buckets due to client error") from exc

    def list_all_objects(self, bucket: str) -> list:
        """
        This method lists all the objects available on a specific bucket.

        Args:
            bucket (str): The name of the bucket where all objects to be listed

        Raises:
            RuntimeError: If the valid credentials are not entered
            or if the role is not given the right permisisons.

        Returns:
            _type_: returns a list of objects in a bucket
        """
        try:
            return list(self.s3_resource.Bucket(bucket).objects.all())
        except ClientError as exc:
            raise RuntimeError(f"Could not list objects for {bucket} bucket.") from exc

    def read_object(
        self,
        s3_object_path: str,
        df_type: str,
        file_type: str,
        spark: "pyspark.sql.SparkSession" = None,
        glueContext=None,
        **kwargs,
    ):
        """
        This method reads a file in a bucket and converts to either a pandas df, spark df
        or a DynamicDataframe.
        (The spark and DynamicDataframe can only be called via a virtual core or glue end point.)

        Args:
            s3_object_path (str): This is the s3 uri
                (for spark and Glue, user needs to edit the uri to 's3a' instead of 's3')
            df_type (str): pandas, spark or glue df type in quotes.
            file_type (str): the file type in quotes eg: "csv"
            spark (pyspark.sql.SparkSession, optional):
                Define the spark session and read into the method in order to read directly
                from the bucket. Defaults to None.
            glueContext (optional): Define the glue_context spark_session and read into the method
                in order to read directly from the bucket. Defaults to None.

        Raises:
            RuntimeError: when file format specified or s3 uri is incorrect
        Returns:
            a pandas, spark or Dynamic dataframe.
        """
        try:
            if df_type == "pandas":
                import pandas as pd

                pandas_ = {
                    "csv": pd.read_csv,
                    "json": pd.read_json,
                    "parquet": pd.read_parquet,
                    "orc": pd.read_orc,
                    "xml": pd.read_xml,
                    "fwf": pd.read_fwf,
                    "sas": pd.read_sas,
                    "spss": pd.read_spss,
                    "html": pd.read_html,
                    "stata": pd.read_stata,
                    "feather": pd.read_feather,
                    "pickle": pd.read_pickle,
                }
                return pandas_[file_type](s3_object_path, **kwargs)
            elif df_type == "spark":
                spark_ = {
                    "parquet": spark.read.parquet,
                    "orc": spark.read.orc,
                    "json": spark.read.json,
                    "csv": spark.read.csv,
                    "text": spark.read.text,
                }
                return spark_[file_type](s3_object_path, **kwargs)
            elif df_type == "glue":
                return glueContext.create_dynamic_frame.from_options(
                    connection_type="s3",
                    connection_options={"paths": [f"{s3_object_path}"]},
                    format=f"{file_type}",
                    format_options={**kwargs},
                )
            else:
                raise NotImplementedError
        except FileNotFoundError as exc:
            raise RuntimeError("Please check if s3 uri is correct or if the file format is entered correctly!") from exc

    def put_object(self, data: Any, bucket: str, key: str) -> None:
        """
        This method takes the specified test_files on memory, the file name, bucket name
        and stores the test_files into the s3 bucket.

        Args:
            data (Any): file on memory.
            bucket (str): name of the bucket to upload file.
            key (str): name of the file.
        """
        try:
            object_ = self.s3_resource.Object(bucket, key)
            object_.put(Body=data)
        except self.s3_exceptions as exc:
            raise RuntimeError(f"Could not write test_files to {key} file in {bucket} bucket.") from exc

    def validate_object(self, bucket: str, key: str) -> bool:
        """
        Checks if the object exist within the bucket.

        Args:
            bucket (str): name of the bucket to check for the file.
            key (str): name of the object to search for

        Raises:
            RuntimeError: if object cannot be validated
        Returns:
            bool: returns True if obect exist in the bucket.
        """
        try:
            # Check if the object exists in target via GET request:
            self.s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            raise RuntimeError(f"Invalid Bucket/Key. Could not validate file '{key}' in '{bucket}' bucket.") from exc

    def write_to_s3_from_local(
        self,
        bucket: str,
        file_path: str,
        s3_key: str,
        extra_args: dict = None,
        callback: callable = None,
        config: boto3.s3.transfer.TransferConfig = None,
    ) -> None:
        """This method write a file from the local machine into the S3 bucket.

        Args:
            bucket (str): The name of the bucket to write to.
            file_path (str): The path to the file to write.
            s3_key (str): The name of the key to write to.
            extra_args (dict, optional): Extra arguments that may be passed to the client operation.
            For allowed upload arguments see boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS.
            Defaults to None.
            callback (func, optional): A method which takes a number of bytes transferred
            to be periodically called during the upload.
            Defaults to None.
            config (optional): The transfer configuration to be used when performing the transfer.
            Defaults to None.

        Raises:
            RuntimeError: when local file cannot be written to S3
        """
        try:
            self.s3_resource.Bucket(bucket).upload_file(file_path, s3_key, extra_args, callback, config)
        except self.s3_exceptions as exc:
            raise RuntimeError(f"Could not write {file_path} to {bucket} bucket.") from exc

    def validate_bucket(self, bucket: str) -> bool:
        """
        This method validates if speficied bucket exist or not

        Args:
            bucket (str): Name of the bucket to be validated

        Returns:
            bool: whether it's True or False if the specified bucket exist
        """
        return self.s3_resource.Bucket(bucket) in self.list_buckets()

    def move_object(self, source: dict, destination_bucket: str, destination_key: str) -> None:
        """
        This method moves an object from a source bucket/key to a new bucket/key (copy then delete)
        Args:
            source (dict): A dictionary contains {'Bucket', 'Key', 'VersionId'} keys and values.
            'VersionId' is optional
            destination_bucket (str): Name of the destination bucket
            destination_key (str): Name of the destination key
        """
        logging.info("Move is not directly supported by boto3, we are performing copy and delete instead.")
        self.copy_object(source, destination_bucket, destination_key)
        self.delete_object(source["Bucket"], source["Key"])

    def copy_object(self, copy_source: dict, destination_bucket: str, destination_key: str) -> None:
        """
        This method makes copy of an object from a source bucket/key and put it in a new destination
        bucket/key
        Args:
            copy_source (dict): A dictionary contains {'Bucket', 'Key', 'VersionId'} keys & values.
            'VersionId' is optional
            destination_bucket (str): Name of the destination bucket
            destination_key (str): Name of the destination key
        """
        source_bckt = copy_source["Bucket"]
        source_key = copy_source["Key"]
        try:
            self.validate_object(bucket=source_bckt, key=source_key)
            self.s3_resource.meta.client.copy(copy_source, destination_bucket, destination_key)
            self.validate_object(destination_bucket, destination_key)
        except self.s3_exceptions as exc:
            raise RuntimeError(
                f"Could not copy {copy_source} to {destination_bucket} bucket for {destination_key} key."
            ) from exc

    def delete_object(self, bucket: str, key: str) -> None:
        """
        This method deletes an object in a specified bucket with a specified key
        Args:
            bucket (str): Name of the bucket where object is located
            key (str): Name of the object key
        """
        try:
            self.validate_object(bucket=bucket, key=key)
            self.s3_resource.Object(bucket, key).delete()
        except self.s3_exceptions as exc:
            raise RuntimeError(f"Could not delete object {key} from {bucket} bucket.") from exc

    def delete_all_objects(self, bucket: str) -> None:
        """
        This method deletes all objects located in a specified bucket (empty the bucket)
        Args:
            bucket (str): Name of the bucket to be emptied
        """
        try:
            objects = self.list_all_objects(bucket)
            for object_ in objects:
                self.delete_object(bucket, object_.key)
        except self.s3_exceptions as exc:
            raise RuntimeError(f"Could not delete all the objects in {bucket}") from exc

    def move_all_objects(self, bucket: str, destination_bucket: str, destination_key: str = None) -> None:
        """
        This method moves all objects from a source bucket to a different location (bucket/key).
        Make copy and empty the source ebucket
        Args:
            bucket (str): Name of the bucket where objects are located originally
            destination_bucket (str): Name of the bucket where objects are to be moved into
            destination_key (str, optional): Name of the folder/sub-folder (if applicable)
            where objects are to be moved into within the specified bucket. Defaults to None.
        """
        try:
            self.copy_all_objects(bucket, destination_bucket, destination_key)
            self.delete_all_objects(bucket)
        except self.s3_exceptions as exc:
            raise RuntimeError(f"Failed to move objects from {bucket} to {destination_bucket}") from exc

    def copy_all_objects(self, bucket: str, destination_bucket: str, destination_key: str = None) -> None:
        """
        This method makes copies of all objects in a specified source bucket and put them into a new
        destination bucket/key without removing them from source
        Args:
            bucket (str): Name of the source bucket where objects are located
            destination_bucket (str): Name of the destination bucket where objects are copied into
            destination_key (str, optional): Name of the folder/sub-folder (if applicable) where
            objects are to be copied into within the specified bucket. Defaults to None.
        """
        try:
            objects = self.list_all_objects(bucket)
            if self.validate_bucket(destination_bucket):
                source = {"Bucket": bucket}
                for object_ in objects:
                    source["Key"] = object_.key
                    if destination_key and len(destination_key) > 0:
                        self.copy_object(
                            source,
                            destination_bucket,
                            destination_key + "/" + source["Key"],
                        )
                    else:
                        self.copy_object(source, destination_bucket, source["Key"])
            else:
                raise RuntimeError(f"Bucket '{destination_bucket}' does not exist.")
        except self.s3_exceptions as exc:
            raise RuntimeError(f"Failed to copy objects from {bucket} to {destination_bucket}") from exc

    def get_object_tag(self, bucket: str, key: str, **kwargs) -> dict:
        """This method gets all the tags exist in a specified object

        Args:
            bucket (str): Name of the bucket where object is located
            key (str): Name of the key of the object

        Returns:
            dict: dictionary in which under key "TagSet" is a list of all key-value pairs for
            existing tags
        """
        try:
            self.validate_object(bucket=bucket, key=key)
            return self.s3_client.get_object_tagging(Bucket=bucket, Key=key, **kwargs)
        except self.s3_exceptions as exc:
            raise RuntimeError(f"Failed to get tag for {key} object in {bucket} bucket") from exc

    def add_object_tag(self, bucket: str, key: str, new_tag: dict, **kwargs) -> None:
        """This method adds a specific tag to a specified object

        Args:
            bucket (str): Name of the bucket where object is located
            key (str): Name of the key of the object where tag needs to be set
            new_tag (dict): A dictionary contains {'Key', 'Value'} keys and values
            representing tag key and tag value

        Raises:
            ValueError: when new_tag argument passed in does not include "Key" and "Value" keys
        """
        try:
            if isinstance(new_tag, dict):
                response = self.get_object_tag(bucket, key, **kwargs)
                response["TagSet"].append(new_tag)
                self.set_object_tag(bucket, key, response["TagSet"])
            else:
                raise TypeError('new_tag argument must be a dictionary of "Key" and "Value" of tags to be added')
        except self.s3_exceptions as exc:
            raise RuntimeError(f"Failed to add tag for {key} object in {bucket} bucket") from exc

    def delete_object_tag(self, bucket: str, key: str, tag: dict, **kwargs) -> None:
        """This method deletes a specific tag in a specified object

        Args:
            bucket (str): Name of the bucket where object is located
            key (str): Name of the key of the object where tag needs to be deleted
            tag (dict): A dictionary contains {'Key', 'Value'} keys and values
            representing tag key and tag value

        Raises:
            RuntimeError: when the tag provided does not exist in the specified object
        """
        try:
            self.validate_object(bucket=bucket, key=key)
            object_tag_list = self.get_object_tag(bucket, key, **kwargs)["TagSet"]
            if tag in object_tag_list:
                new_tag_set = []
                for dict_ in object_tag_list:
                    if dict_["Key"] != tag["Key"]:
                        new_tag_set.append(dict_)
                    else:
                        continue
                self.set_object_tag(bucket, key, new_tag_set)
            else:
                raise RuntimeError("Tag to be deleted is not valid or not exist in current object")
        except self.s3_exceptions as exc:
            raise RuntimeError(f"Could not delete tag for object {key} in {bucket} bucket.") from exc

    def set_object_tag(self, bucket: str, key: str, tag_set: list[dict]) -> None:
        """This method set a list of tag for a specified object

        Args:
            bucket (str): Name of the bucket where object is located
            key (str): Name of the key of the object where tag needs to be set
            tag_set (list[dict]): a list of all the tag set dictionary containing {"Key", "Value"}
            keys and values representing tag key and tag value

        Raises:
            ValueError: when tag_set argument passed in is not list or tuple
            ValueError: when tag_set argument passed in does not include "Key" and "Value" keys
            ValueError: when tag_set argument passed in contain a tag Key that are duplicated
        """
        try:
            self.validate_object(bucket=bucket, key=key)
            if isinstance(tag_set, (list, tuple)):
                try:
                    self.s3_client.put_object_tagging(Bucket=bucket, Key=key, Tagging={"TagSet": tag_set})
                except ParamValidationError as exc:
                    raise ValueError(
                        'tag_set argument must include dictionary of "Key" and "Value" of the tag'
                    ) from exc
                except ClientError as exc:
                    raise ValueError("Cannot have multiple Tags with the same key within an object") from exc
            else:
                raise TypeError("tag_set argument must be a list or tuple of sets of tags to be added")
        except self.s3_exceptions as exc:
            raise RuntimeError(f"Could not set tag for object {key} in {bucket} bucket.") from exc

    def set_bucket_tag(self, bucket: str, tag_set: list[dict], **kwargs) -> None:
        """This method set up tags for the specified bucket given a set of tags

        Args:
            bucket (str): Name of the bucket where tags need to be set
            tag_set (list[dict]): a list of all the tag set dictionary containing {"Key", "Value"}
            keys and values representing tag key and tag value

        Raises:
            ValueError: when tag_set argument passed in is not list or tuple
            ValueError: when tag_set argument passed in does not include "Key" and "Value" keys
            ValueError: when tag_set argument passed in contain a tag Key that are duplicated
        """
        try:
            if self.validate_bucket(bucket=bucket):
                if isinstance(tag_set, (list, tuple)):
                    try:
                        self.s3_client.put_bucket_tagging(Bucket=bucket, Tagging={"TagSet": tag_set}, **kwargs)
                    except ParamValidationError as exc:
                        raise ValueError(
                            'tag_set argument must have dictionary of "Key" and "Value" of the tag'
                        ) from exc
                    except ClientError as exc:
                        raise ValueError("Cannot have multiple Tags with the same key within an object") from exc
                else:
                    raise TypeError("tag_set argument must be a list or tuple of sets of tags to be added")
            else:
                raise RuntimeError(f"Bucket '{bucket}' does not exist.")
        except self.s3_exceptions as exc:
            raise RuntimeError(f"Could not set tag for {bucket} bucket.") from exc

    def get_bucket_tag(self, bucket: str, **kwargs) -> dict:
        """This method gets all the tags exist in the specified bucket

        Args:
            bucket (str): Name of the bucket to get tags

        Returns:
            dict: dictionary in which under key "TagSet" is a list of all key-value pairs for
            existing tags
        """
        try:
            if self.validate_bucket(bucket=bucket):
                return self.s3_client.get_bucket_tagging(Bucket=bucket, **kwargs)
            else:
                raise RuntimeError(f"Bucket '{bucket}' does not exist.")
        except ClientError:
            return {"TagSet": []}
        except self.s3_exceptions as exc:
            raise RuntimeError(f"Could not get tag for {bucket} bucket.") from exc

    def delete_bucket_tag(self, bucket: str, **kwargs) -> None:
        """This method deletes all the tags of the specified bucket

        Args:
            bucket (str): Name of bucket to have all tags be delete
        """
        try:
            if self.validate_bucket(bucket=bucket):
                self.s3_client.delete_bucket_tagging(Bucket=bucket, **kwargs)
            else:
                raise RuntimeError(f"Bucket '{bucket}' does not exist.")
        except self.s3_exceptions as exc:
            raise RuntimeError(f"Could not delete tag for {bucket} bucket.") from exc
