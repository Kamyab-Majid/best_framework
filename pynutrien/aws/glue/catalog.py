from awsglue.context import GlueContext
from awsglue import DynamicFrame

class GlueIO:

    def __init__(self, glue_context, redshift_tmp_dir=None):
        self.glue_context = glue_context
        self._redshift_tmp_dir = redshift_tmp_dir
        self._catalog_args = None
        self._fmt = None
        self._ctx = None

    def format(self, fmt):
        self._fmt = fmt
        return self

    def transformation_ctx(self, ctx):
        self._ctx = ctx
        return self

    def catalog(self, database, table):
        self._catalog_args = {'database': database, 'table': table}
        return self

    def _is_initialized(self, error=True):
        if not (isinstance(self.glue_context, GlueContext) and
                isinstance(self._catalog_args, dict) and
               'database' in self._catalog_args and
               'table' in self._catalog_args):
            if error:
                raise RuntimeError(f"{self.__class__.__name__} was not initialized properly: call {self.__class__.__name__}.catalog(...)")
            return False
        return True

class GlueReader(GlueIO):
    _auto_ctx = 0

    def dynamic_frame(self, **kwargs):
        self._is_initialized()

        if self._ctx is None:
            self._ctx = f"read_df_{GlueReader._auto_ctx}"
            GlueReader._auto_ctx += 1

        txId = self.glue_context.start_transactions(read_only=False)
        if 'additional_options' in kwargs:
            if not isinstance(kwargs['additional_options'], dict):
                raise TypeError("Expected 'additional_options' to be a dictionary")
            if 'transactionId' not in kwargs['additional_options']:
                kwargs['additional_options']['transactionId'] = txId

        dynf = self.glue_context.create_dynamic_frame.from_catalog(
            database=self._catalog_args['database'],  # TODO: the docs call this name_space now?
            table_name=self._catalog_args['table'],
            transformation_ctx=self._ctx,
            **kwargs  # support query
        )
        self.glue_context.commit_transaction(txId)
        return dynf

    def dataframe(self, **kwargs):
        return self.dynamic_frame(**kwargs).toDF()

    def view(self, name=None):
        if name is None: name = self._catalog_args['table']
        df = self.dataframe()
        df.createOrReplaceTempView(name)
        return df

    def pandas(self, **kwargs):
        return self.dataframe(**kwargs).toPandas()

class GlueWriter(GlueIO):
    _auto_ctx = 0

    def dynamic_frame(self, dynf, **kwargs):
        self._is_initialized()

        if self._ctx is None:
            self._ctx = f"write_df_{GlueWriter._auto_ctx}"
            GlueWriter._auto_ctx += 1

        txId = self.glue_context.start_transactions(read_only=False)
        if 'additional_options' in kwargs:
            if not isinstance(kwargs['additional_options'], dict):
                raise TypeError("Expected 'additional_options' to be a dictionary")
            if 'transactionId' not in kwargs['additional_options']:
                kwargs['additional_options']['transactionId'] = txId

        self.glue_context.write_dynamic_frame.from_catalog(
            frame=dynf,
            database=self._catalog_args['database'],  # TODO: the docs call this name_space now?
            table_name=self._catalog_args['table'],
            transformation_ctx=self._ctx,
            redshift_tmp_dir=self._redshift_tmp_dir,
            **kwargs
        )
        self.glue_context.commit_transaction(txId)
        return txId

    def dataframe(self, df, **kwargs):
        dynf = DynamicFrame(df, self.glue_context, f"create_df_{GlueWriter._auto_ctx}")
        self.dynamic_frame(dynf, **kwargs)

    def pandas(self, pdf, **kwargs):
        df = self.glue_context.spark_session.createDataFrame(pdf)
        self.dataframe(df, **kwargs)

