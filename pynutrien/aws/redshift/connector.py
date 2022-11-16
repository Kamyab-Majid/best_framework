import redshift_connector
import logging
import sys


class RedshiftConnector:
    """
    This class acts as a Redshift connector that will connect to Amazon Redshift and allow user executing SQL SELECT from Redshift database
    """

    def __init__(self, **kwargs) -> None:
        """
        Initializes the class - setting up AWS attributes for Redshift connection
        """
        # set up connection attributes
        self.host = kwargs["host"]
        self.database = kwargs["database"]
        self.user = kwargs["user"]
        self.password = kwargs["password"]
        self.iam = kwargs["iam"]
        self.db_user = kwargs["db_user"]
        self.cluster_identifier = kwargs["cluster_identifier"]
        self.profile = kwargs["profile"] if kwargs["profile"] else 'default'

        # set up logger
        self.logger = kwargs["logger"] if kwargs["logger"] else logging.getLogger(
            __name__)
        self.handler = kwargs["handler"] if kwargs["handler"] else logging.StreamHandler(
            sys.stdout)
        self.logging_level = kwargs["logging_level"] if kwargs["logging_level"] else 20
        self.format_string = '%(asctime)s::%(module)s::%(function_name)s::%(lineno)d::%(message)s'
        self.logger_format = logging.Formatter(
            kwargs["logger_format"] if kwargs["logger_format"] else self.format_string)

        self.handler.setFormatter(self.logger_format)
        self.logger.setLevel(self.logging_level)
        self.logger.addHandler(self.handler)

        # define connection
        self.connection = self.create_connection_using_IAM_credentials(
        ) if self.iam else self.create_connection_using_AWS_credentials()
        if self.connection:
            with self.connection as conn:
                self.cursor = conn.cursor()

    def create_connection_using_AWS_credentials(self):
        """
        This method creates connection to Amazon Redshift cluster using AWS credentials
        """
        try:
            return redshift_connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
        except Exception as e:
            self.logger.exception(
                "Connection using AWS credentials can't be reached. Please check your input AWS credentials")

    def create_connection_using_IAM_credentials(self):
        """
        This method creates connection to Amazon Redshift cluster using IAM credentials
        """
        try:
            return redshift_connector.connect(
                iam=self.iam,
                database=self.database,
                db_user=self.db_user,
                password=self.password,
                user=self.user,
                cluster_identifier=self.cluster_identifier,
                profile=self.profile
            )
        except Exception as e:
            self.logger.exception(
                "Connection using IAM credentials can't be reached. Please check your input IAM credentials")

    def redshift_execute_select(self, table: str, columns: str = "*"):
        """
        This method allows users executing SQL SELECT queries by providing the specific table and columns that users would like to select from
        Args:
            table (str): name of the table to select from
            columns (str): name of the column(s) to be selected from the specified table. If multiple, use comma (",") as a delimiter to separate each column name in the string
        """
        with self.cursor as cursor:
            try:
                cursor.execute(f"SELECT {columns} FROM {table}")
                result = cursor.fetchall()
                if result.empty:
                    self.logger.info("Return dataset is empty")
                return result
            except Exception as e:
                self.logger.exception(
                    "Select query cannot be executed. Please check if your input is sufficient.")
