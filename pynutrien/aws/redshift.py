from __future__ import annotations

import logging

import redshift_connector
from redshift_connector.core import Connection
from redshift_connector.error import InterfaceError, ProgrammingError


class RedshiftConnector:
    """
    This class acts as a Redshift connector that will connect to Amazon Redshift
    and allow user executing SQL SELECT from Redshift database
    """

    def __init__(self, **kwargs) -> None:
        """
        Initializes the class - setting up AWS attributes for Redshift connection
        Args:
            (Required for connection using AWS credentials):
                host (str): The name of the database instance to connect to.
                database (str): The name of the database instance to connect to.
                user (str): The username to use for authentication with the Amazon Redshift cluster.
                password (str):
                    The password to use for authentication with the Amazon Redshift cluster.

            (Required for connection using IAM credentials - when 'iam' is set to True):
                iam (bool): If IAM authentication is enabled. Defaults to False.
                database (str): The name of the database instance to connect to.
                db_user (str): The user ID to use with Amazon Redshift
                password (str):
                    The password to use for authentication with the Amazon Redshift cluster.
                user (str): The username to use for authentication with the Amazon Redshift cluster.
                cluster_identifier (str): The cluster identifier of the Amazon Redshift cluster.
                profile (str): The profile of the Amazon Redshift cluster. Defaults to 'default'.
            (Optional):
                autocommit (bool): If autocommit is enabled (set to True). Defaults to False.
        """
        # set up connection attributes
        self.host = kwargs["host"] if "host" in kwargs and kwargs["host"] else None
        self.database = kwargs["database"] if "database" in kwargs and kwargs["database"] else None
        self.user = kwargs["user"] if "user" in kwargs and kwargs["user"] else None
        self.password = kwargs["password"] if "password" in kwargs and kwargs["password"] else None
        self.iam = kwargs["iam"] if "iam" in kwargs and kwargs["iam"] else False
        self.db_user = kwargs["db_user"] if "db_user" in kwargs and kwargs["db_user"] else None
        self.cluster_identifier = (
            kwargs["cluster_identifier"] if "cluster_identifier" in kwargs and kwargs["cluster_identifier"] else None
        )
        self.profile = kwargs["profile"] if "profile" in kwargs and kwargs["profile"] else "default"
        self.autocommit = kwargs["autocommit"] if "autocommit" in kwargs and kwargs["autocommit"] else False

        # define connection
        self.connection = None
        try:
            self.connection = (
                self.create_connection_using_IAM_credentials()
                if self.iam
                else self.create_connection_using_AWS_credentials()
            )
        except InterfaceError as intf_err:
            err_code = intf_err.args[1].args[0]
            if err_code == 11001:
                # self.logger.exception(
                # "Connection cannot be reached. Please check your input AWS credentials.")
                raise RuntimeError(
                    "Connection cannot be reached. Please check your input AWS credentials."
                ) from intf_err
            if err_code == 10060:
                # self.logger.exception(
                # "Connection cannot be reached. The cluster is not accessible.")
                raise RuntimeError("Connection cannot be reached. The cluster is not accessible.") from intf_err

        # turn on autocommit if autocommit is set (default is off)
        if self.connection and self.autocommit:
            self.connection.rollback()
            self.connection.autocommit = True
            self.connection.run("VACUUM")

    def create_connection_using_AWS_credentials(self) -> Connection:
        """
        This method creates connection to Amazon Redshift cluster using AWS credentials
        This method is initiated when the class is initiated (default)

        Raises:
            RuntimeError: when required arguments to connect using AWS credentials are missing

        Returns:
            Connection: A Connection object associated with the specified Amazon Redshift cluster
        """
        if self.host and self.database and self.user and self.password:
            return redshift_connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
            )
        raise RuntimeError(
            "Missing required arguments. Please make sure arguments and values exist for \
                'host', 'database', 'user', and 'password'"
        )

    def create_connection_using_IAM_credentials(self) -> Connection:
        """
        This method creates connection to Amazon Redshift cluster using IAM credentials
        This method is initiated when the class is initiated (with argument 'iam' is set to True)

        Raises:
            RuntimeError: when required arguments to connect using IAM credentials are missing

        Returns:
            Connection: A Connection object associated with the specified Amazon Redshift cluster
        """
        if self.database and self.db_user and self.password and self.user and self.cluster_identifier:
            return redshift_connector.connect(
                iam=self.iam,
                database=self.database,
                db_user=self.db_user,
                password=self.password,
                user=self.user,
                cluster_identifier=self.cluster_identifier,
                profile=self.profile,
            )
        raise RuntimeError(
            "Missing required arguments. Please make sure arguments and values exist for \
                'database','db_user', 'password', 'user', 'cluster_identifier'"
        )

    def redshift_execute_select(self, table: str, columns: str = "*") -> tuple:
        """
        This method allows users to execute SQL SELECT queries by providing the specific
        table and columns that users would like to select from

        Args:
            table (str): The name of the table to select from.
            columns (str, optional):
                The name of the column(s) to be selected from the specified table.
                Defaults to "*" to select all columns.
                Use comma (",") to separate column name if there're multiple

        Raises:
            RuntimeError: when table and/or columns arguments passed in do not exist/ not valid
            RuntimeError: when connection is not yet established at the beginning

        Returns:
            tuple: Result of test_files displayed in a tuple of lists (rows of test_files).
            If test_files is empty, it will be logged as an info for user's awareness.
        """
        if self.connection:
            with self.connection as conn:
                with conn.cursor() as cursor:
                    try:
                        cursor.execute(f"SELECT {columns} FROM {table}")
                        result = cursor.fetchall()
                        if len(result) == 0:
                            logging.info("Return dataset is empty")
                        return result
                    except ProgrammingError as prog_err:
                        raise RuntimeError(prog_err.args[0]["M"]) from prog_err
        raise RuntimeError("Connection has not yet established.")

    def redshift_execute_multiple_queries(self, table: str, query_statements: list) -> tuple:
        """
        This method allows users to execute multiple queries to Redshift table providing a list of
        query statements

        Args:
            table (str): The name of the table to execute queries from.
            query_statements (list): The list of query statements to be executed

        Raises:
            RuntimeError: when connection is not yet established at the beginning

        Returns:
            tuple: Result of final test_files displayed in a tuple of lists (rows of test_files)
            after queries are executed
        """
        if self.connection:
            with self.connection as conn:
                with conn.cursor() as cursor:
                    for query in query_statements:
                        cursor.execute(query)
                    cursor.execute(f"SELECT * FROM {table}")
                    return cursor.fetchall()
        raise RuntimeError("Connection has not yet established.")

    def redshift_execute_insert(
        self, table: str, source_table: str = None, source_columns: str = "*", values: str = None
    ) -> tuple:
        """
        This method allows users to execute SQL INSERT queries

        Args:
            table (str): The name of the table to insert to.
            source_table (str, optional): The name of the table used as source. Defaults to None.
            source_columns (str, optional): The name of the columns used as source. Defaults to "*".
            values (str, optional): Custom values to be inserted to table. Defaults to None.

        Raises:
            RuntimeError: when required arguments to run INSERT query are missing

        Returns:
            tuple: Result of final test_files displayed in a tuple of lists (rows of test_files)
            after queries are executed (for testing)
        """
        query_statements = []
        if values is None and source_table is None:
            raise RuntimeError(
                "Missing required arguments. Please make sure to include arguments for\
                    either source table/columns or specific values to be inserted."
            )
        if source_table:
            query_statements.append(f"INSERT INTO {table} (SELECT {source_columns} FROM {source_table})")
        if values:
            query_statements.append(f"INSERT INTO {table} VALUES {values}")

        return self.redshift_execute_multiple_queries(table=table, query_statements=query_statements)

    def redshift_execute_delete(self, table: str, reference_tables: str = None, conditions: str = None) -> tuple:
        """
        This method allows users to execute SQL DELETE queries

        Args:
            table (str): The name of the table to be deleted from.
            reference_tables (str, optional): The name of the table list introduced
            when additional tables are referenced in the WHERE clause condition.
            Defaults to None.
            conditions (str, optional): The condition can be a restriction on a column,
            a join condition, or a condition based on the result of a query.
            Defaults to None.

        Raises:
            RuntimeError: when a required argument is missing
            (missing conditions where there're reference tables specified)

        Returns:
            tuple: Result of final test_files displayed in a tuple of lists (rows of test_files)
            after queries are executed (for testing)
        """
        query_statements = []
        if reference_tables:
            if conditions:
                query_statements.append(f"DELETE FROM {table} USING {reference_tables} WHERE {conditions}")
            else:
                raise RuntimeError("Conditions argument needs to be specified for reference tables.")
        else:
            if conditions:
                query_statements.append(f"DELETE FROM {table} WHERE {conditions}")
            else:
                query_statements.append(f"DELETE FROM {table}")
        return self.redshift_execute_multiple_queries(table=table, query_statements=query_statements)
