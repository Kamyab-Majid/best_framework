# pylint: disable=missing-module-docstring, redefined-outer-name, missing-function-docstring, logging-fstring-interpolation, invalid-name, no-else-return, too-many-arguments, unused-argument, singleton-comparison

import os

import boto3
import pytest
import pandas as pd
from pyspark.sql import SparkSession

from dotenv import load_dotenv

from pynutrien.utility.s3_operations import S3Operations

bucket_name = "insights-framework-test"
non_existing_bucket_name = "non-existing-bucket"


@pytest.fixture
def s3_ops():
    load_dotenv()

    aws_access_key_id = os.getenv(('AWS_ACCESS_KEY'))
    aws_secret_access_key = os.getenv(('AWS_SECRET_ACCESS_KEY'))

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)

    return S3Operations(session=session)

@pytest.fixture
def local_files_list():
    return [
        "enterprise_survey.csv",
        "file_uploaded_from_function.csv",
        "flavors.csv",
        "json_file_uploaded_from_function.json",
        "new_json_file_uploaded_from_out_object_function.json",
        "sample_CustomersOrders.xml",
        "sample_taxi_data.parquet",
        "text.txt"
    ]
    
@pytest.fixture
def json_file():
    return """
{
    "fruit": {
        "apple": {
            "size": "small",
            "color": "red",
            "country": "USA"
        },
        "banana":{
            "size": "medium",
            "color": "yellow",
            "country": "Fiji"
        },
        "orange":{
            "size": "large",
            "color": "orange",
            "country": "Egypt"
        }
    }
}
"""

@pytest.fixture
def upload_file_name():
    return "test_file_from_pytest.json"

@pytest.fixture
def upload_direct_file_name():
    return "pytest_enterprise_survey_test_writing.csv"

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

@pytest.fixture
def non_existing_object():
    return {
        'Bucket':bucket_name,
        'Key':'non_existing_obj.txt'
    }

@pytest.fixture
def existing_object_to_copy():
    return {
        'Bucket': bucket_name,
        'Key': 'flavors.csv'
    }

@pytest.fixture
def copy_to_destination():
    return {
        'Bucket': bucket_name,
        'Key': 'copy-destination/flavors.csv'
    }

@pytest.fixture
def existing_object_to_move():
    return {
        'Bucket': bucket_name,
        'Key': 'sample_taxi_data.parquet'
    }

@pytest.fixture
def move_to_destination():
    return {
        'Bucket': bucket_name,
        'Key': 'move-destination/sample_taxi_data.parquet'
    }

@pytest.fixture
def existing_object_to_delete():
    return {
        'Bucket': bucket_name,
        'Key': 'text.txt'
    }

@pytest.fixture
def tag_set():
    return [
        {
            'Key': 'tag_key_1',
            'Value': 'tag_value_1'
        },
        {
            'Key': 'tag_key_2',
            'Value': 'tag_value_2'
        }
    ]

@pytest.fixture
def new_tag():
    return {
        'Key': 'tag_key_3',
        'Value': 'tag_value_3'
    }

def objects_list_in_bucket(s3_ops, bucket_name):
    objs = s3_ops.list_all_objects(bucket_name)
    res = []
    for obj_ in objs:
        res.append(obj_.key)
    return res

# test_list_buckets
def test_list_of_buckets(s3_ops):
    result = s3_ops.list_buckets()
    assert isinstance(result, list)

# test_list_all_objects
def test_list_of_objects_in_bucket(s3_ops):
    result = s3_ops.list_all_objects(bucket_name)
    assert isinstance(result, list)

# test_read_object
def test_pandas_df_using_csv(s3_ops):
    data = s3_ops.read_object("s3://insights-framework-test/flavors.csv", "pandas", "csv")
    assert isinstance(data, pd.DataFrame)

def test_exception_error_if_file_does_not_exist_for_pandas_df(s3_ops):
    with pytest.raises(RuntimeError):
        s3_ops.read_object("s3://insights-framework-test/flavorss.csv", "pandas", "csv")

def test_spark_df_using_csv():
    pass

# test_put_object
def test_file_get_uploaded_to_bucket(s3_ops, json_file, upload_file_name):
    s3_ops.put_object(json_file, bucket_name, upload_file_name)
    result = s3_ops.validate_object(bucket_name, upload_file_name)
    assert result == True

def test_put_object_to_non_existing_bucket_should_raise_runtime_error(
    s3_ops, json_file, upload_file_name):
    with pytest.raises(RuntimeError):
        s3_ops.put_object(json_file, "non_existing_bckt", upload_file_name)

# test_write_to_s3_from_local
def test_direct_write_into_s3_bucket(s3_ops, upload_direct_file_name):
    s3_ops.write_to_s3_from_local(
        bucket_name,
       "tests/s3_test_files/enterprise_survey_test_writing.csv",
        upload_direct_file_name)
    result = s3_ops.validate_object(bucket_name, upload_direct_file_name)
    assert result == True

def test_write_to_s3_from_non_existing_local_file_should_raise_runtime_error(
    s3_ops, upload_direct_file_name
):
    with pytest.raises(FileNotFoundError):
        s3_ops.write_to_s3_from_local(
            bucket_name,
            "tests/s3_test_files/non_existing_file.csv",
            upload_direct_file_name
        )

# test_move_object
def test_move_non_existing_object_should_raise_runtime_error(
    s3_ops, non_existing_object, move_to_destination):
    destination_bucket = move_to_destination['Bucket']
    destination_key = move_to_destination['Key']
    with pytest.raises(RuntimeError):
        s3_ops.move_object(
            source=non_existing_object,
            destination_bucket=destination_bucket,
            destination_key=destination_key)

def test_move_existing_object_should_make_object_exist_in_destination_and_not_exist_in_source_after(
        s3_ops, existing_object_to_move, move_to_destination):
    destination_bucket = move_to_destination['Bucket']
    destination_key = move_to_destination['Key']
    s3_ops.move_object(
        source=existing_object_to_move,
        destination_bucket=destination_bucket,
        destination_key=destination_key)
    existing_objs = objects_list_in_bucket(s3_ops, bucket_name)
    assert existing_object_to_move['Key'] not in existing_objs
    assert destination_key in existing_objs
    s3_ops.write_to_s3_from_local(
        bucket=bucket_name,
        file_path="tests/s3_test_files/sample_taxi_data.parquet",
        s3_key='sample_taxi_data.parquet'
        )

def \
    test_move_existing_object_to_a_non_existing_destination_should_still_work(
        s3_ops, existing_object_to_move):
    destination_key = 'non-exist-folder/sample_taxi_data.parquet'
    s3_ops.move_object(
        source=existing_object_to_move,
        destination_bucket=bucket_name,
        destination_key=destination_key)
    existing_objs = objects_list_in_bucket(s3_ops, bucket_name)
    assert existing_object_to_move['Key'] not in existing_objs
    assert destination_key in existing_objs
    s3_ops.write_to_s3_from_local(
        bucket=bucket_name,
        file_path="tests/s3_test_files/sample_taxi_data.parquet",
        s3_key='sample_taxi_data.parquet'
        )
    s3_ops.s3_resource.Object(bucket_name, destination_key).delete()

# test_copy_object
def test_copy_non_existing_object_should_raise_runtime_error(
    s3_ops,non_existing_object,copy_to_destination):
    destination_bucket = copy_to_destination['Bucket']
    destination_key = copy_to_destination['Key']
    with pytest.raises(RuntimeError):
        s3_ops.move_object(
            source=non_existing_object,
            destination_bucket=destination_bucket,
            destination_key=destination_key)

def \
    test_copy_existing_object_should_make_object_exist_in_both_destination_and_source_after(
        s3_ops, existing_object_to_copy, copy_to_destination):
    destination_bucket = copy_to_destination['Bucket']
    destination_key = copy_to_destination['Key']
    s3_ops.copy_object(
        copy_source=existing_object_to_copy,
        destination_bucket=destination_bucket,
        destination_key=destination_key)
    existing_objs = objects_list_in_bucket(s3_ops, bucket_name)
    assert existing_object_to_copy['Key'] in existing_objs
    assert destination_key in existing_objs

def \
    test_copy_existing_object_to_a_non_existing_destination_should_still_work(
        s3_ops, existing_object_to_copy):
    destination_key = 'non-exist-folder/flavors.csv'
    s3_ops.copy_object(
        copy_source=existing_object_to_copy,
        destination_bucket=bucket_name,
        destination_key=destination_key)
    existing_objs = objects_list_in_bucket(s3_ops, bucket_name)
    assert existing_object_to_copy['Key'] in existing_objs
    assert destination_key in existing_objs
    s3_ops.s3_resource.Object(bucket_name, destination_key).delete()

# test_delete_object
def test_delete_non_existing_object_should_raise_runtime_error(
    s3_ops,non_existing_object):
    with pytest.raises(RuntimeError):
        s3_ops.delete_object(
            bucket=non_existing_object['Bucket'],
            key=non_existing_object['Key'])

def test_delete_existing_object_should_make_object_not_exist_anymore_after(
    s3_ops,
    existing_object_to_delete):
    bucket = existing_object_to_delete['Bucket']
    key = existing_object_to_delete['Key']
    s3_ops.delete_object(
        bucket=bucket,
        key=key)
    existing_objs = objects_list_in_bucket(s3_ops, bucket_name)
    assert key not in existing_objs
    s3_ops.write_to_s3_from_local(
        bucket=bucket_name,
        file_path="tests/s3_test_files/text.txt",
        s3_key='text.txt'
        )


# test_delete_all_objects
def \
    test_delete_all_objects_should_make_all_objects_in_bucket_not_exist_anymore_after(
        s3_ops, local_files_list):
    existing_objs = objects_list_in_bucket(s3_ops, bucket_name)
    assert len(existing_objs) > 0
    s3_ops.delete_all_objects(bucket=bucket_name)
    existing_objs_after_delete = objects_list_in_bucket(s3_ops, bucket_name)
    assert len(existing_objs_after_delete) == 0
    for file_name in local_files_list:
        s3_ops.write_to_s3_from_local(
            bucket=bucket_name,
            file_path=f"tests/s3_test_files/{file_name}",
            s3_key=file_name
            )

def test_delete_all_objects_from_a_non_existing_bucket_should_raise_runtime_error(s3_ops):
    with pytest.raises(RuntimeError):
        s3_ops.delete_all_objects(bucket="non_existing_bckt")


# test_move_all_objects
def \
    test_move_all_objects_should_make_objects_not_exist_in_source_but_exist_in_destination_after(
        s3_ops, local_files_list):
    destination_bucket = bucket_name+"-copied"
    existing_objs_in_source_bucket_before_moved = objects_list_in_bucket(s3_ops, bucket_name)
    assert len(existing_objs_in_source_bucket_before_moved) > 0
    s3_ops.move_all_objects(
        bucket=bucket_name,
        destination_bucket=destination_bucket
    )
    existing_objs_in_source_bucket_after_moved = objects_list_in_bucket(s3_ops, bucket_name)
    assert len(existing_objs_in_source_bucket_after_moved) == 0
    existing_objs_in_destination_bucket = objects_list_in_bucket(s3_ops, destination_bucket)
    assert set(existing_objs_in_source_bucket_before_moved)\
        .issubset(set(existing_objs_in_destination_bucket)) == True
    for file_name in local_files_list:
        s3_ops.write_to_s3_from_local(
            bucket=bucket_name,
            file_path=f"tests/s3_test_files/{file_name}",
            s3_key=file_name
            )
    s3_ops.delete_all_objects(bucket=destination_bucket)

def test_move_all_objects_from_a_non_existing_bucket_should_raise_runtime_error(s3_ops):
    destination_bucket = bucket_name+"-copied"
    with pytest.raises(RuntimeError):
        s3_ops.move_all_objects(
            bucket="non_existing_bckt",
            destination_bucket=destination_bucket)

def test_move_all_objects_to_a_non_existing_destination_bucket_should_raise_runtime_error(
    s3_ops):
    destination_bucket = "non_existing_bckt"
    with pytest.raises(RuntimeError):
        s3_ops.move_all_objects(
            bucket=bucket_name,
            destination_bucket=destination_bucket)


# test_copy_all_objects
def \
    test_copy_all_objects_should_make_objects_exist_in_both_source_bucket_and_in_destination_after(
        s3_ops):
    destination_bucket = bucket_name+"-copied"
    existing_objs_in_source_bucket_before_copied = objects_list_in_bucket(s3_ops, bucket_name)
    assert len(existing_objs_in_source_bucket_before_copied) > 0
    s3_ops.copy_all_objects(
        bucket=bucket_name,
        destination_bucket=destination_bucket
    )
    existing_objs_in_source_bucket_after_copied = objects_list_in_bucket(s3_ops, bucket_name)
    assert existing_objs_in_source_bucket_after_copied \
        == existing_objs_in_source_bucket_before_copied
    existing_objs_in_destination_bucket = objects_list_in_bucket(s3_ops, destination_bucket)
    assert set(existing_objs_in_source_bucket_before_copied)\
        .issubset(set(existing_objs_in_destination_bucket)) == True

def test_copy_all_objects_from_a_non_existing_bucket_should_raise_runtime_error(s3_ops):
    destination_bucket = bucket_name+"-copied"
    with pytest.raises(RuntimeError):
        s3_ops.copy_all_objects(
            bucket="non_existing_bckt",
            destination_bucket=destination_bucket)

def test_copy_all_objects_to_a_non_existing_destination_bucket_should_raise_runtime_error(
    s3_ops):
    destination_bucket = "non_existing_bckt"
    with pytest.raises(RuntimeError):
        s3_ops.copy_all_objects(
            bucket=bucket_name,
            destination_bucket=destination_bucket)


#test_set_object_tag and test_get_objetc_tag
def test_set_object_tag_should_make_tag_exist_in_object(s3_ops, existing_object_to_copy, tag_set):
    s3_ops.set_object_tag(
        bucket=bucket_name,
        key=existing_object_to_copy["Key"],
        tag_set=tag_set
    )
    existing_tags_list_in_obj = s3_ops.get_object_tag(
        bucket=bucket_name,
        key=existing_object_to_copy["Key"]
    )["TagSet"]
    assert tag_set == existing_tags_list_in_obj

def test_set_object_tag_on_non_existing_object_should_raise_runtime_error(
    s3_ops, non_existing_object):
    with pytest.raises(RuntimeError):
        s3_ops.set_object_tag(
            bucket=bucket_name,
            key=non_existing_object["Key"],
            tag_set=tag_set
        )

def test_set_object_tag_without_passing_appropriate_tag_set_in_list_should_raise_type_error(
    s3_ops, existing_object_to_copy):
    with pytest.raises(ValueError):
        s3_ops.set_object_tag(
            bucket=bucket_name,
            key=existing_object_to_copy["Key"],
            tag_set=[{"WrongKey": "tag_key_1", "Value": "tag_val_2"}])

    with pytest.raises(TypeError):
        s3_ops.set_object_tag(
            bucket=bucket_name,
            key=existing_object_to_copy["Key"],
            tag_set="invalid tag set")

# test_add_object_tag
def test_add_tag_with_valid_key_and_value_should_create_tag_for_that_object_appropriately(
    s3_ops, existing_object_to_copy, new_tag):
    s3_ops.add_object_tag(
        bucket=bucket_name,
        key=existing_object_to_copy["Key"],
        new_tag=new_tag)
    existing_tags_list_in_obj = s3_ops.get_object_tag(
        bucket=bucket_name,
        key=existing_object_to_copy["Key"]
    )["TagSet"]
    assert new_tag in existing_tags_list_in_obj

def test_add_tag_that_has_existing_key_should_raise_value_error(
    s3_ops, existing_object_to_copy, new_tag):
    existing_tags_list_in_obj = s3_ops.get_object_tag(
        bucket=bucket_name,
        key=existing_object_to_copy["Key"]
    )["TagSet"]
    if new_tag not in existing_tags_list_in_obj:
        s3_ops.add_object_tag(
            bucket=bucket_name,
            key=existing_object_to_copy["Key"],
            new_tag=new_tag
        )

    with pytest.raises(ValueError):
        s3_ops.add_object_tag(
            bucket=bucket_name,
            key=existing_object_to_copy["Key"],
            new_tag=new_tag
        )

def test_add_tag_with_new_tag_passed_in_is_not_dict_type_should_raise_type_error(
    s3_ops, existing_object_to_copy):
    with pytest.raises(TypeError):
        s3_ops.add_object_tag(
            bucket=bucket_name,
            key=existing_object_to_copy["Key"],
            new_tag="invalid tag")

def \
    test_add_tag_that_does_not_have_appropriate_key_pair_value_should_raise_value_error(
    s3_ops, existing_object_to_copy):
    with pytest.raises(ValueError):
        s3_ops.add_object_tag(
            bucket=bucket_name,
            key=existing_object_to_copy["Key"],
            new_tag={"WrongKey": "tag_key_1", "Value": "tag_val_2"})


#test_get_object_tag
def test_get_tag_for_non_existing_object_should_raise_runtime_error(
    s3_ops, non_existing_object):
    with pytest.raises(RuntimeError):
        s3_ops.get_object_tag(
            bucket=bucket_name,
            key=non_existing_object["Key"]
        )

def test_get_object_tag_where_there_is_no_tag_should_return_empty(s3_ops, existing_object_to_move):
    current_tags = s3_ops.get_object_tag(
        bucket=bucket_name,
        key=existing_object_to_move["Key"]
    )
    assert current_tags["TagSet"] == []

#test_delete_object_tag
def test_delete_non_exisiting_tag_should_raise_runtime_error(
    s3_ops,existing_object_to_move, new_tag):
    with pytest.raises(RuntimeError):
        s3_ops.delete_object_tag(
            bucket=bucket_name,
            key=existing_object_to_move["Key"],
            tag=new_tag
        )

def test_delete_tag_that_does_not_have_appropriate_key_value_pair_should_raiseruntime_error(
    s3_ops,existing_object_to_move):
    with pytest.raises(RuntimeError):
        s3_ops.delete_object_tag(
            bucket=bucket_name,
            key=existing_object_to_move["Key"],
            tag={"WrongKey": "tag_key_1", "Value": "tag_val_2"}
        )

def test_delete_tag_that_is_not_passed_in_as_dict_type_should_raise_runtime_error(
    s3_ops,existing_object_to_move):
    with pytest.raises(RuntimeError):
        s3_ops.delete_object_tag(
            bucket=bucket_name,
            key=existing_object_to_move["Key"],
            tag="not valid tag"
        )

def test_delete_existing_tag_should_make_tag_not_exist_in_object_anymore(
    s3_ops,existing_object_to_copy):
    tag_to_delete={
        'Key': 'tag_key_1',
        'Value': 'tag_value_1'
    }
    existing_tags_list_in_obj = s3_ops.get_object_tag(
        bucket=bucket_name,
        key=existing_object_to_copy["Key"]
    )["TagSet"]
    assert tag_to_delete in existing_tags_list_in_obj

    s3_ops.delete_object_tag(
        bucket=bucket_name,
        key=existing_object_to_copy["Key"],
        tag=tag_to_delete
    )
    existing_tags_list_in_obj_after_delete_tag = s3_ops.get_object_tag(
        bucket=bucket_name,
        key=existing_object_to_copy["Key"]
    )["TagSet"]
    assert tag_to_delete not in existing_tags_list_in_obj_after_delete_tag


#test_set_bucket_tag and test_get_bucket_tag
def test_set_bucket_tag_should_make_tag_exist_in_bucket(s3_ops, tag_set):
    s3_ops.set_bucket_tag(bucket=bucket_name, tag_set=tag_set)
    existing_tags_list_in_bckt = s3_ops.get_bucket_tag(
        bucket=bucket_name
    )["TagSet"]
    assert tag_set == existing_tags_list_in_bckt

def test_set_bucket_tag_on_non_existing_bucket_should_raise_runtime_error(s3_ops, tag_set):
    with pytest.raises(RuntimeError):
        s3_ops.set_bucket_tag(bucket="non_existing_bucket", tag_set=tag_set)

def test_set_bucket_tag_without_passing_appropriate_tag_set_in_list_should_raise_error(
    s3_ops):
    with pytest.raises(ValueError):
        s3_ops.set_bucket_tag(
            bucket=bucket_name,
            tag_set=[{"WrongKey": "tag_key_1", "Value": "tag_val_2"}])

    with pytest.raises(TypeError):
        s3_ops.set_bucket_tag(
            bucket=bucket_name,
            tag_set="invalid tag set")


#test_delete_bucket_tag
def test_delete_bucket_tag_should_make_bucket_have_no_tags(s3_ops, tag_set):
    s3_ops.set_bucket_tag(bucket=bucket_name, tag_set=tag_set)
    s3_ops.delete_bucket_tag(bucket=bucket_name)
    existing_tags_list_in_bckt_after_delete_tag = s3_ops.get_bucket_tag(
        bucket=bucket_name)["TagSet"]
    assert len(existing_tags_list_in_bckt_after_delete_tag) == 0

def test_delete_tag_for_non_existing_bucket_should_raise_runtime_error(s3_ops):
    with pytest.raises(RuntimeError):
        s3_ops.delete_bucket_tag(bucket="non_existing_bucket", tag_set=tag_set)


#test_get_bucket_tag
def test_get_bucket_tag_for_non_existing_bucket_should_raise_runtime_error(s3_ops):
    with pytest.raises(RuntimeError):
        s3_ops.get_bucket_tag(bucket="non_existing_bucket")

def test_get_bucket_tag_if_bucket_has_no_tag_should_return_empty_tagset_value(s3_ops):
    s3_ops.delete_bucket_tag(bucket=bucket_name)
    tag_list_after_delete = s3_ops.get_bucket_tag(bucket=bucket_name)["TagSet"]
    assert len(tag_list_after_delete) == 0
