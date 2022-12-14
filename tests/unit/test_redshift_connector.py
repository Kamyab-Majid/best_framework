from __future__ import annotations

import os

import pytest

from pynutrien.aws.redshift import RedshiftConnector

# from dotenv import load_dotenv


table_name = "category"
staged_table_name = "category_stage"


@pytest.fixture
def non_accessible_cluster_config():
    # load_dotenv()
    redshift_user = os.getenv("REDSHIFT_USER")
    redshift_password = os.getenv("REDSHIFT_PASSWORD")
    return {
        "host": "redshift-cluster-1.cydu5eacdk8z.ca-central-1.redshift.amazonaws.com",
        "database": "dev",
        "user": redshift_user,
        "password": redshift_password,
    }


@pytest.fixture
def accessible_cluster_config():
    # load_dotenv()
    redshift_user = os.getenv("REDSHIFT_USER")
    redshift_password = os.getenv("REDSHIFT_PASSWORD")
    return {
        "host": "nutrien-insights-redshift-cluster-test.cydu5eacdk8z.ca-central-1.redshift.amazonaws.com",
        "database": "dev",
        "user": redshift_user,
        "password": redshift_password,
        "db_user": redshift_user,
        "cluster_identifier": "nutrien-insights-redshift-cluster-test",
    }


@pytest.fixture
def wrong_cluster_config():
    return {"host": "there is a typo here", "user": "oops", "password": "typo"}


@pytest.fixture
def conn(accessible_cluster_config):
    return RedshiftConnector(
        host=accessible_cluster_config["host"],
        database=accessible_cluster_config["database"],
        user=accessible_cluster_config["user"],
        password=accessible_cluster_config["password"],
    )


@pytest.fixture
def correct_return_data_when_select_query():
    return {
        "all": (
            [1, "Sports", "MLB", "Major League Baseball"],
            [3, "Sports", "NFL", "National Football League"],
            [6, "Shows", "Musicals", "Musical theatre"],
            [8, "Shows", "Opera", "All opera and light opera"],
            [11, "Concerts", "Classical", "All symphony, concerto, and choir concerts"],
            [2, "Sports", "NHL", "National Hockey League"],
            [4, "Sports", "NBA", "National Basketball Association"],
            [5, "Sports", "MLS", "Major League Soccer"],
            [7, "Shows", "Plays", "All non-musical theatre"],
            [9, "Concerts", "Pop", "All rock and pop music concerts"],
            [10, "Concerts", "Jazz", "All jazz singers and bands"],
        ),
        "catid, catname": (
            [2, "NHL"],
            [4, "NBA"],
            [5, "MLS"],
            [7, "Plays"],
            [9, "Pop"],
            [10, "Jazz"],
            [1, "MLB"],
            [3, "NFL"],
            [6, "Musicals"],
            [8, "Opera"],
            [11, "Classical"],
        ),
        "after_conditional_delete": (
            [11, "Concerts", "Classical", "All symphony, concerto, and choir concerts"],
            [10, "Concerts", "Jazz", "All jazz singers and bands"],
        ),
        "after_conditional_delete_referencing_other_table": (
            [2, "Sports", "NHL", "National Hockey League"],
            [4, "Sports", "NBA", "National Basketball Association"],
            [5, "Sports", "MLS", "Major League Soccer"],
            [7, "Shows", "Plays", "All non-musical theatre"],
            [10, "Concerts", "Jazz", "All jazz singers and bands"],
            [1, "Sports", "MLB", "Major League Baseball"],
            [3, "Sports", "NFL", "National Football League"],
            [6, "Shows", "Musicals", "Musical theatre"],
            [8, "Shows", "Opera", "All opera and light opera"],
            [11, "Concerts", "Classical", "All symphony, concerto, and choir concerts"],
        ),
    }


# test_initiatie_connection


def test_creating_connection_to_accessible_redshift_cluster_using_AWS_credentials(accessible_cluster_config):
    conn = None
    conn = RedshiftConnector(
        host=accessible_cluster_config["host"],
        database=accessible_cluster_config["database"],
        user=accessible_cluster_config["user"],
        password=accessible_cluster_config["password"],
    )
    assert conn.connection is not None
    conn.close()


def test_creating_connection_to_accessible_redshift_cluster_using_IAM_credentials(accessible_cluster_config):
    conn = None
    conn = RedshiftConnector(
        iam=True,
        database=accessible_cluster_config["database"],
        user=accessible_cluster_config["user"],
        password=accessible_cluster_config["password"],
        db_user=accessible_cluster_config["db_user"],
        cluster_identifier=accessible_cluster_config["cluster_identifier"],
    )
    assert conn.connection is not None
    conn.close()


def test_creating_connection_with_wrong_credentials_or_cluster_config_should_raise_runtime_error(wrong_cluster_config):
    with pytest.raises(RuntimeError):
        RedshiftConnector(host=wrong_cluster_config["host"])

    with pytest.raises(RuntimeError):
        RedshiftConnector(iam=True, user=wrong_cluster_config["user"], password=wrong_cluster_config["password"])


def test_creating_connection_to_non_accessible_redshift_cluser_should_raise_runtime_error(
    non_accessible_cluster_config,
):
    with pytest.raises(RuntimeError):
        RedshiftConnector(
            host=non_accessible_cluster_config["host"],
            database=non_accessible_cluster_config["database"],
            user=non_accessible_cluster_config["user"],
            password=non_accessible_cluster_config["password"],
        )


# test_redshift_execute_select


def test_querying_select_from_redshift_should_return_correct_data(conn, correct_return_data_when_select_query):
    res = conn.redshift_execute_select(table=table_name)
    conn.close()
    assert sorted(res) == sorted(correct_return_data_when_select_query["all"])


def test_querying_select_certain_columns_should_return_data_from_that_column_only(
    conn, correct_return_data_when_select_query
):
    res = conn.redshift_execute_select(columns="catid, catname", table=table_name)
    conn.close()
    assert sorted(res) == sorted(correct_return_data_when_select_query["catid, catname"])


def test_querying_select_from_a_non_existing_table_or_column_should_raise_runtime_error(conn):
    with pytest.raises(RuntimeError):
        conn.redshift_execute_select(table="non_existing")


# test_redshift_execute_insert


def test_querying_insert_to_redshift_from_a_source_table(conn, correct_return_data_when_select_query):
    data_after_inserted = conn.redshift_execute_insert(table=staged_table_name, source_table=table_name)
    conn.close()
    assert sorted(data_after_inserted) == sorted(correct_return_data_when_select_query["all"])


def test_querying_insert_to_redshift_using_specified_custom_values(conn):
    data_after_inserted = conn.redshift_execute_insert(
        table=staged_table_name,
        values="\
            (12, 'Concerts', 'Comedy', 'All stand-up comedy performances'), \
            (13, 'Concerts', 'Other', default)",
    )
    conn.close()
    expected_data = (
        [12, "Concerts", "Comedy", "All stand-up comedy performances"],
        [13, "Concerts", "Other", "General"],
    )
    assert sorted(data_after_inserted) == sorted(expected_data)


def test_querying_insert_to_redshift_using_a_source_table_and_custom_values_at_the_same_time(
    conn, correct_return_data_when_select_query
):
    data_after_inserted = conn.redshift_execute_insert(
        table=staged_table_name,
        source_table=table_name,
        values="\
            (12, 'Concerts', 'Comedy', 'All stand-up comedy performances'), \
            (13, 'Concerts', 'Other', default)",
    )
    conn.close()
    expected_data = (
        [12, "Concerts", "Comedy", "All stand-up comedy performances"],
        [13, "Concerts", "Other", "General"],
        *correct_return_data_when_select_query["all"],
    )
    assert sorted(data_after_inserted) == sorted(expected_data)


def test_querying_insert_with_missing_argunments_should_raise_runtime_error(conn):
    with pytest.raises(RuntimeError):
        conn.redshift_execute_insert(table=table_name)


# test_redshift_execute_delete


def test_querying_delete_from_redshift_without_specifying_any_conditions(conn):
    data_after_deleted = conn.redshift_execute_delete(table=table_name)
    conn.close()
    assert len(data_after_deleted) == 0


def test_querying_delete_from_redshift_using_specified_conditions_but_no_reference_table(
    conn, correct_return_data_when_select_query
):
    data_after_deleted = conn.redshift_execute_delete(table=table_name, conditions="catid between 0 and 9")
    conn.close()
    expected_data = correct_return_data_when_select_query["after_conditional_delete"]
    assert sorted(data_after_deleted) == sorted(expected_data)


def test_querying_delete_from_redshift_using_specified_conditions_from_a_reference_table(
    conn, correct_return_data_when_select_query
):
    data_after_deleted = conn.redshift_execute_delete(
        table=table_name, reference_tables="event", conditions="event.catid=category.catid and category.catid=9"
    )
    conn.close()
    expected_data = correct_return_data_when_select_query["after_conditional_delete_referencing_other_table"]
    assert sorted(data_after_deleted) == sorted(expected_data)


def test_querying_delete_with_missing_argunments_should_raise_runtime_error(conn):
    with pytest.raises(RuntimeError):
        conn.redshift_execute_delete(table=table_name, reference_tables="event")
