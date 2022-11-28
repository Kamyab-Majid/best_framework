from awsglue.context import GlueContext
from awsglue import DynamicFrame


class GlueIO:
    """This is the class that initializes the dynamicFrame."""

    def __init__(self, glue_context, redshift_tmp_dir=None):
        """
        This is the init method that initializes GlueIO object to read datacatalog
        into DynamicFrames or pandas df

        Args:
            glue_context (str): Pass the gluecontext session.
            redshift_tmp_dir (str, optional): pass the redshift credentials to store the
            data frame. Defaults to None.
        """
        self.glue_context = glue_context
        self._redshift_tmp_dir = redshift_tmp_dir
        self._catalog_args = None
        self._fmt = None
        self._ctx = None

    def format(self, fmt):
        """
        Define the format for AWS Glue connection that supports multiple formats

        Args:
            fmt (str): A format specification (optional).

        Returns:
            str: specified format of the file.
        """
        self._fmt = fmt
        return self

    def transformation_ctx(self, ctx):
        """
        A unique string that is used to identify state information (optional).

        Args:
            ctx (str): A unique string that is used to identify state information (optional).

        Returns:
            str: A unique string that is used to identify state information (optional).
        """
        self._ctx = ctx
        return self

    def catalog(self, database, table):
        """
        A string that defines the database and table name

        Args:
            database (str): Name of target database.
            table (str): Name of target table.

        Returns:
            _type_: _description_
        """
        self._catalog_args = {"database": database, "table": table}
        return self

    def _is_initialized(self, error=True):
        """
        This method verifies if catalog exists within the defined
        data catalog.

        Args:
            error (bool, optional): Defaults to True.

        Raises:
            RuntimeError: If the table and db names are wrong the we
            raise this error.

        Returns:
            bool: True/False if instance exists._
        """
        if not (
            isinstance(self.glue_context, GlueContext)
            and isinstance(self._catalog_args, dict)
            and "database" in self._catalog_args
            and "table" in self._catalog_args
        ):
            if error:
                raise RuntimeError(
                    f"{self.__class__.__name__} was not initialized properly: call {self.__class__.__name__}.catalog(...)"
                )
            return False
        return True


class GlueReader(GlueIO):
    """
    Glue Reader class that reads the data catalog from AWS.

    Args:
        GlueIO (object): pass the initialized glue object.

    Returns:
        DynamicDataframe: an AWS Dynamic Data Frame.
    """

    _auto_ctx = 0

    def dynamic_frame(self, **kwargs):
        """
        checks if the dynamic frame exists by calling the is_initialized() method.

        Returns:
            bool: True or false.
        """
        self._is_initialized()

        if self._ctx is None:
            self._ctx = f"read_df_{GlueReader._auto_ctx}"
            GlueReader._auto_ctx += 1

        txId = self.glue_context.begin_transaction(read_only=False)
        if "additional_options" in kwargs:
            if not isinstance(kwargs["additional_options"], dict):
                raise TypeError("Expected 'additional_options' to be a dictionary")
            if "transactionId" not in kwargs["additional_options"]:
                kwargs["additional_options"]["transactionId"] = txId

        dynf = self.glue_context.create_dynamic_frame.from_catalog(
            database=self._catalog_args[
                "database"
            ],  # TODO: the docs call this name_space now?
            table_name=self._catalog_args["table"],
            transformation_ctx=self._ctx,
            **kwargs,  # support query
        )
        self.glue_context.commit_transaction(txId)
        return dynf

    def dataframe(self, **kwargs):
        """Create a dynamicData frame and converts to dataframe.

        Returns:
            pandas dataframe: converts table to dataframe.
        """
        return self.dynamic_frame(**kwargs).toDF()

    def view(self, name=None):
        """Makes a view of the table.

        Args:
            name (str, optional): name of table. Defaults to None.

        Returns:
            pandas dataframe: converts table to dataframe.
        """
        if name is None:
            name = self._catalog_args["table"]
        df = self.dataframe()
        df.createOrReplaceTempView(name)
        return df

    def pandas(self, **kwargs):
        """Converts dataframe to pandas dataframe.

        Returns:
            pandas dataframe: converts table to dataframe.
        """
        return self.dataframe(**kwargs).toPandas()


class GlueWriter(GlueIO):
    """
    Glue writer class that writes into the data catalog from AWS.

    Args:
        GlueIO (object): pass the initialized glue object.
    """

    _auto_ctx = 0

    def dynamic_frame(self, dynf, **kwargs):
        """
        checks if the dynamic frame exists by calling the is_initialized() method.

        Returns:
            bool: True or false.
        """
        self._is_initialized()

        if self._ctx is None:
            self._ctx = f"write_df_{GlueWriter._auto_ctx}"
            GlueWriter._auto_ctx += 1

        txId = self.glue_context.begin_transaction(read_only=False)
        if "additional_options" in kwargs:
            if not isinstance(kwargs["additional_options"], dict):
                raise TypeError("Expected 'additional_options' to be a dictionary")
            if "transactionId" not in kwargs["additional_options"]:
                kwargs["additional_options"]["transactionId"] = txId

        self.glue_context.write_dynamic_frame.from_catalog(
            frame=dynf,
            database=self._catalog_args[
                "database"
            ],  # TODO: the docs call this name_space now?
            table_name=self._catalog_args["table"],
            transformation_ctx=self._ctx,
            redshift_tmp_dir=self._redshift_tmp_dir,
            **kwargs,
        )
        self.glue_context.commit_transaction(txId)
        return txId

    def dataframe(self, df, **kwargs):
        """Create a DynamicFrame.

        Returns:
            pandas dataframe: converts table to dataframe.
        """
        dynf = DynamicFrame(df, self.glue_context, f"create_df_{GlueWriter._auto_ctx}")
        self.dynamic_frame(dynf, **kwargs)

    def pandas(self, pdf, **kwargs):
        """Converts dataframe to pandas dataframe.

        Returns:
            pandas dataframe: converts table to dataframe.
        """
        df = self.glue_context.spark_session.createDataFrame(pdf)
        self.dataframe(df, **kwargs)


if __name__ == "__main__":

    from pynutrien.aws.glue.job import GlueSparkContext

    context = GlueSparkContext()
    Reader = GlueReader(context.glue_context, None)
    df = (
        Reader.catalog("crawler_test", "caseware_customer_a")
        .transformation_ctx("cases")
        .dataframe()
    )
    df.show()
