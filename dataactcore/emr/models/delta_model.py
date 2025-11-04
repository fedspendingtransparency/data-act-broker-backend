import os
import logging
import pandas as pd
import polars as pl
from abc import ABC
from argparse import ArgumentTypeError
from typing import Callable

from deltalake import DeltaTable, QueryBuilder, Field, schema
from deltalake.exceptions import TableNotFoundError

from pyspark.sql import DataFrame, SparkSession

from dataactbroker.helpers.aws_helpers import get_aws_credentials
from dataactcore.config import CONFIG_BROKER


logger = logging.getLogger(__name__)

def get_storage_options():
    """ DeltaLake library doesn't use boto3 and doesn't pull the aws creds the same way. """
    aws_creds = get_aws_credentials()
    return {
            "AWS_ACCESS_KEY_ID": aws_creds.access_key,
            "AWS_SECRET_ACCESS_KEY": aws_creds.secret_key,
            "AWS_REGION": "us-gov-west-1",
            "AWS_SESSION_TOKEN": aws_creds.token
    }

class DeltaModel(ABC):
    s3_bucket: str
    database: str
    table_name: str
    pk: str
    unique_constraints: [(str,)]
    migration_history: [str]

    def __init__(self, spark=None):
        self.spark = spark

        try:
            self.dt = DeltaTable(self.table_path, storage_options=get_storage_options())
        except TableNotFoundError:
            self.dt = None

    def exists(self):
        return self.dt is not None

    @property
    def database_path(self):
        return f's3://{self.s3_bucket}/data/delta/{self.database}'

    @property
    def database_path_hadoop(self):
        return f's3a://{self.s3_bucket}/data/delta/{self.database}'

    @property
    def table_path(self):
        return f's3://{self.s3_bucket}/data/delta/{self.database}/{self.table_name}'

    @property
    def table_path_hadoop(self):
        return f's3a://{self.s3_bucket}/data/delta/{self.database}/{self.table_name}'

    @property
    def table_ref(self):
        return f'{self.database}.{self.table_name}'

    @property
    def structure(self):
        """
        The schema/structure of the delta table as StructType with StructFields
        """
        raise NotImplementedError("Structure/schema not implemented.")

    def to_pandas_df(self):
        return self.dt.to_pyarrow_table().to_pandas()

    def to_polars_df(self):
        return pl.from_arrow(self.dt.to_pyarrow_table())

    def initialize(self, recreate=False):
        logger.info(f'Initializing {self.table_ref}')
        self._register_table_hive(recreate=recreate)
        # if not self.dt:
        #     self.dt = DeltaTable(self.table_path, storage_options=get_storage_options())
        # else:
        #     logger.info(f'{self.table_path} already initialized')

    def _register_table_hive(self, recreate=False):
        self.spark.sql(rf"""
            CREATE DATABASE IF NOT EXISTS {self.database}
            LOCATION '{self.database_path_hadoop}'
        """)
        df = self.spark.createDataFrame([], self.structure)
        if recreate:
            (
            df.write.format("delta")
                .option("path", self.table_path_hadoop)
                .option("overwriteSchema", "true")
                .mode("overwrite")
                .saveAsTable(self.table_ref)
            )
        else:
            (
            df.write.format("delta")
                .mode("ignore")
                .option("path", self.table_path_hadoop)
                .saveAsTable(self.table_ref)
            )
        # TODO: This *should* allow one to run `ALTER TABLE DROP COLUMN ...` commands
        #       but we ran into issues when trying it.
        # self.spark.sql(f"""
        #     ALTER TABLE {self.table_ref} SET TBLPROPERTIES (
        #       'delta.minReaderVersion' = '2',
        #       'delta.minWriterVersion' = '5',
        #       'delta.columnMapping.mode' = 'name'
        #     )
        # """)

    def migrate(self, start=0):
        """
        start (int): starting index of the migration list to run
            0 - all migrations
            -1 - last migration
        """
        migrations_dir = os.path.join(CONFIG_BROKER["path"], 'dataactcore', 'emr', 'migrations')
        for migration in self.migration_history[start:]:
            path = os.path.join(migrations_dir, f'{migration}.sql')
            logger.info(f'Running migration {path} on {self.table_ref}')
            if not os.path.exists(path):
                raise FileNotFoundError(f'Migration {migration} not found.')
            with open(path, "r") as f:
                spark_sql = f.read()
            if spark_sql:
                self.spark.sql(spark_sql)
            else:
                logger.info(f'No SQL found in {path}.')



    @property
    def repopulate_query(self):
        """
        Can be a string or returning a callable, dataframe.
        If the text is too large for the model, pull the text from a separate script.
        """
        raise NotImplementedError("Repopulate query not implemented.")

    def repopulate(self):
        self.load_query(self.repopulate_query)

    @property
    def increment_query(self):
        """
        Can be a string or returning a callable, dataframe.
        If the text is too large for the model, pull the text from a separate script.
        """
        raise NotImplementedError("Increment query not implemented.")

    def increment(self):
        self.load_query(self.increment_query)

    def load_query(self, query: str | Callable[[SparkSession], DataFrame]):
        if isinstance(query, str):
            self.spark.sql(query)
        elif isinstance(query, Callable):
            (
                query(self.spark)
                    .write.format("delta")
                    .mode("overwrite")
                    .option("path", self.table_path_hadoop)
                    .saveAsTable(self.table_ref)
            )
        else:
            raise ArgumentTypeError(f"Invalid query. `{query}` must be a string or a Callable.")

    def merge(self, df: [pd.DataFrame, pl.DataFrame]):
        if isinstance(df, pd.DataFrame):
            df = pl.from_pandas(df)

        if not self.dt:
            raise Exception('Table not instantiated')

        self.dt.merge(
            source=df,
            predicate=f"s.{self.pk} = t.{self.pk}",
            source_alias="s",
            target_alias="t"
        ) \
            .when_matched_update_all() \
            .when_not_matched_insert_all() \
            .execute()

