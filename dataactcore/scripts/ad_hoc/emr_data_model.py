import boto3
import logging
import pandas as pd
import polars as pl
from abc import ABC, abstractmethod
from datetime import date, datetime

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import DEFC

from pyspark.sql import SparkSession
# from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, ArrayType, BooleanType, DateType,
#                                DecimalType, NullType, TimestampType)

# from delta import *
# from delta.tables import DeltaTable
from deltalake.schema import ArrayType, PrimitiveType
from deltalake import DeltaTable, Field, schema
from deltalake.writer import write_deltalake
from deltalake.exceptions import TableNotFoundError

from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

logger = logging.getLogger(__name__)

# class ColumnType(ABC):
#     @abstractmethod
#     def __init__(self):
#         pass

# class Text(ColumnType):
#     def __init__(self):
#         self.default = ''
#         self.type = str
#
# class Integer(ColumnType):
#     def __init__(self):
#         self.default = 0
#         self.type = int
#
# class Double(ColumnType):
#     def __init__(self):
#         self.default = 0.0
#         self.type = float
#
# class Boolean(ColumnType):
#     def __init__(self):
#         self.default = False
#         self.type = bool
#
# class Array(ColumnType):
#     def __init__(self, type):
#         self.item_type = type
#         self.default = []
#         self.type = list
#
# class Date(ColumnType):
#     def __init__(self, type):
#         self.default = date(1970, 1, 1)
#         self.type = date
#
# class DateTime(ColumnType):
#     def __init__(self, type):
#         self.default = datetime(1970, 1, 1, 0, 0, 0)
#         self.type = datetime
#
# class Column():
#     def __init__(self, column_type: ColumnType, default=None, nullable: bool = True, unique: bool = False):
#         self.column_type = column_type
#         if default is not None and not self.column_type.validate(default):
#             raise ValueError(f'Invalid default: {default}')
#         elif (default is not None) or nullable:
#             self.column_type.default = default
#         self.unique = unique
#
#     def validate(self, value):
#         # TODO: uniqueness check
#         return isinstance(value, self.column_type.type)

class DeltaModel(ABC):
    bucket_schema: str
    table_name: str
    pk: str
    unique_constraints: [(str,)]
    # null_constraints: [str]

    def __init__(self, spark=None):
        self.spark = spark

        try:
            self.dt = DeltaTable(self.table_path, storage_options=get_storage_options())
        except TableNotFoundError:
            self.dt = None

    @property
    def s3_bucket(self):
        env = 'qat'
        return f'dti-broker-emr-{env}'

    @property
    def table_path(self):
        return f's3://{self.s3_bucket}/{self.bucket}/{self.bucket_schema}/{self.table_name}/'

    @property
    def structure(self):
        pass

    def initialize_table(self):
        logger.info(f'Initializing {self.table_path}')
        if self.spark:
            # TODO: Breaks due to AWS Glue
            return self.createIfNotExists(self.spark)\
                .tableName(self.table_name)\
                .location(self.table_path)\
                .addColumns(self.structure)\
                .execute()
        else:
            self.dt = DeltaTable.create(
                table_uri=str(self.table_path),
                name=self.table_name,
                schema=self.structure,
                mode='overwrite',
                storage_options=get_storage_options(),
            )
            # empty_df = pl.DataFrame(schema=self.structure)
            # write_deltalake(
            #     str(self.table_path),
            #     empty_df,
            #     mode="overwrite"
            # )

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

        # TODO: Check for dups
        # write_deltalake(
        #     str(self.table_path),
        #     df,
        #     mode="append",
        # )

class DEFCDelta(DeltaModel):
    bucket = 'reference'
    bucket_schema = 'int'
    table_name = 'defc'
    pk = 'defc_id'
    unique_constraints = [('code')]
    # null_constraints = ['code', 'is_valid']

    @property
    def structure(self):
        # SQLAlchemy
        # created_at = Column(DateTime)
        # updated_at = Column(DateTime)
        # defc_id = Column(Integer)
        # code = Column(Text, nullable=False, unique=True)
        # public_laws = Column(Array(Text))
        # public_law_short_titles = Column(Array(Text))
        # group = Column(Text)
        # urls = Column(Array(Text))
        # is_valid = Column(Boolean, nullable=False)
        # earliest_pl_action_date = Column(DateTime)

        # Spark
        # return StructType([
        #     StructField("created_at", TimestampType(), True),
        #     StructField("updated_at", TimestampType(), True),
        #     StructField("defc_id", IntegerType(), True),
        #     StructField("code", StringType(), False),
        #     StructField("public_laws", ArrayType(StringType()), True),
        #     StructField("public_law_short_titles", ArrayType(StringType()), True),
        #     StructField("group", StringType(), True),
        #     StructField("urls", ArrayType(StringType()), True),
        #     StructField("is_valid", BooleanType(), False),
        #     StructField("earliest_pl_action_date", TimestampType(), True),
        # ])

        # pandas
        # return {
        #     'created_at': 'datetime64[ns]',
        #     'updated_at': 'datetime64[ns]',
        #     'defc_id': int,
        #     'code': str,
        #     'public_laws': object, # pandas converts arrays to object
        #     'public_law_short_titles': object, # pandas converts arrays to object
        #     'group': str,
        #     'urls': object, # pandas converts arrays to object
        #     'is_valid': bool,
        #     'earliest_pl_action_date': 'datetime64[ns]'
        # }

        # polars
        # return {
        #     'created_at': pl.Datetime,
        #     'updated_at': pl.Datetime,
        #     'defc_id': pl.Int64,
        #     'code': pl.Utf8,
        #     'public_laws': pl.List(pl.Utf8),
        #     'public_law_short_titles': pl.List(pl.Utf8),
        #     'group': pl.Utf8,
        #     'urls': pl.List(pl.Utf8),
        #     'is_valid': pl.Boolean,
        #     'earliest_pl_action_date': pl.Datetime
        # }

        # polars with JSON?
        # return Schema.from_json('''{
        #     "type": "struct",
        #     "fields": [
        #         {"name": "created_at", "type": "datetime", "nullable": true, "metadata": {}},
        #         {"name": "updated_at", "type": "datetime", "nullable": true, "metadata": {}},
        #         {"name": "defc_id", "type": "integer", "nullable": false, "metadata": {}},
        #         {"name": "code", "type": "string", "nullable": false, "metadata": {}},
        #         {"name": "public_laws", "type": "array", , "elementType": "string", "nullable": true, "metadata": {}},
        #         {"name": "public_law_short_titles", "type": "array", , "elementType": "string", "nullable": true, "metadata": {}},
        #         {"name": "group", "type": "string", "nullable": true, "metadata": {}},
        #         {"name": "urls", "type": "array", "elementType": "string", "nullable": true, "metadata": {}},
        #         {"name": "is_valid", "type": "boolean", "nullable": false, "metadata": {}},
        #         {"name": "earliest_pl_action_date", "type": "datetime", "nullable": true, "metadata": {}},
        #     ]
        # }''')

        # polars with Schema
        return schema.Schema([
            Field('created_at', "timestamp", nullable=True),
            Field('updated_at', "timestamp", nullable=True),
            Field('defc_id', "integer", nullable=False),
            Field('code', "string", nullable=False),
            Field('public_laws', ArrayType(PrimitiveType('string')), nullable=True),
            Field('public_law_short_titles', ArrayType(PrimitiveType("string")), nullable=True),
            Field('group', "string", nullable=True),
            Field('urls', ArrayType(PrimitiveType('string')), nullable=True),
            Field('is_valid', "boolean", nullable=False),
            Field('earliest_pl_action_date', "timestamp", nullable=True),
        ])


def setup_spark():
    # Initialize SparkSession for AWS Glue
    spark = SparkSession.builder \
        .appName("GlueDeltaLakeJob") \
        .enableHiveSupport() \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark

def get_storage_options():
    """ DeltaLake library doesn't use boto3 and doesn't pull the aws creds the same way. """
    session = boto3.Session()
    credentials = session.get_credentials()
    # To get a "frozen" set of credentials (useful for passing to other clients)
    frozen_credentials = credentials.get_frozen_credentials()
    return {
            "AWS_ACCESS_KEY_ID": frozen_credentials.access_key,
            "AWS_SECRET_ACCESS_KEY": frozen_credentials.secret_key,
            "AWS_REGION": "us-gov-west-1",
            "AWS_SESSION_TOKEN": frozen_credentials.token
    }

# Spark - initialize model and schema, hive
# TODO: DUCK DB POPULATION
# TODO: POLARS POPULATION
# AWS Glue? via Boto3?
# MINIO locally
# Focus on streaming data instead of large memory

# What the model looks like base, example table, script used to create table, make schema changes/evolution
# Metastore piece
    # Spark
    # AWS Glue
    # another option hive and glue
# Compare with USAspending Delta Models
# Migrate to Shared Repo

if __name__ == "__main__":
    sess = GlobalDB.db().session
    # spark = setup_spark()
    spark = None

    # setup hive connection with SQLAlchemy
    # engine = create_engine('hive://localhost:10000/default')

    # get a dataframe from the existing postgres as sample data
    defc_df = pd.read_sql_table(DEFC.__table__.name, sess.connection())

    defc_delta_table = DEFCDelta()

    defc_delta_table.initialize_table()

    logger.info('populating it with data')
    defc_delta_table.merge(defc_df)

    logger.info('querying it')
    # data = spark.read.csv("s3://your-s3-bucket/input_data.csv", header=True, inferSchema=True)
    # print(data)
    pulled_df = defc_delta_table.dt.to_pyarrow_table()
    pulled_df_pandas = pulled_df.to_pandas()
    print(pulled_df_pandas)

    # logger.info('updating a value')
    # deltaTable = DeltaTable.replace(spark).tableName("testTable").addColumns(df.schema).execute()
    # defc_delta_table.update("id = 1", {"value": "'new_value'"})

    # deltaTable.update(
    #     condition = "status = 'pending'",
    #     set = { "status": "'processed'" }
    # )
    #
    # print('renaming a col')
    # deltaTable = DeltaTable.create(spark)
    #     .tableName("testTable")
    #     .addColumn("c1", dataType="INT", nullable=False)
    #     .addColumn("c2", dataType=IntegerType(), generatedAlwaysAs="c1 + 1")
    #     .partitionedBy("c1")
    #     .execute()
    #
    # print('deleting a record')
    # deltaTable.delete("id = 123")
    #
    # print('clearing the table')
    # deltaTable.delete("id = 123")
    #
    # print('deleting the table')