import os
import sys
import boto3
import logging
import pandas as pd
import polars as pl
from abc import ABC, abstractmethod
from datetime import date, datetime

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import DEFC
from dataactbroker.helpers.aws_helpers import get_aws_credentials

from dataactbroker.helpers.spark_helper import configure_spark_session, get_active_spark_session


# from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, ArrayType, BooleanType, DateType,
#                                DecimalType, NullType, TimestampType)


# Note: Both delta-spark and deltalake libraries have their own DeltaTable class. Try not to mix them up.

# delta.io / delta-spark package
# from delta import *
# from delta.tables import DeltaTable

# deltalake package
from deltalake.schema import ArrayType, PrimitiveType
from deltalake import DeltaTable, QueryBuilder, Field, schema
from deltalake.writer import write_deltalake
from deltalake.exceptions import TableNotFoundError

from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

from pyhive import hive
# import jaydebeapi

from dataactcore.config import CONFIG_BROKER

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
    s3_bucket: str
    database: str
    table_name: str
    pk: str
    unique_constraints: [(str,)]
    # null_constraints: [str]

    def __init__(self, spark=None, hive=None):
        self.spark = spark
        self.hive = hive

        try:
            self.dt = DeltaTable(self.table_path, storage_options=get_storage_options())
        except TableNotFoundError:
            self.dt = None

    @property
    def table_path(self):
        return f's3://{self.s3_bucket}/data/delta/{self.database}/{self.table_name}'

    @property
    def hadoop_path(self):
        return f's3a://{self.s3_bucket}/data/delta/{self.database}/{self.table_name}'

    @property
    def table_ref(self):
        return f'{self.database}.{self.table_name}'

    @property
    def structure(self):
        pass

    @property
    def columns(self):
        cols = []
        for field in self.structure.fields:
            col_dict = {'Name': field.name}
            if isinstance(field.type, PrimitiveType):
                col_dict['Type'] = field.type.type
            elif isinstance(field.type, ArrayType):
                col_dict['Type'] = 'array'
            cols.append(col_dict)
        return cols

    def initialize_table(self):
        logger.info(f'Initializing {self.table_path}')
        if not self.dt:
            # if self.spark:
                # DeltaTable.createIfNotExists(self.spark)\
                #     .tableName(self.table_name)\
                #     .location(self.table_path)\
                #     .addColumns(self.structure)\
                #     .execute()
                # self.dt = DeltaTable(self.table_path, storage_options=get_storage_options())
            # else:
            self.dt = DeltaTable.create(
                table_uri=str(self.table_path),
                name=self.table_name,
                schema=self.structure,
                mode='overwrite',
                storage_options=get_storage_options(),
            )
            self._register_table_hive()
            # self._create_table_glue()
        else:
            logger.info(f'{self.table_path} already initialized')

    def _register_table_glue(self):
        glue_client = boto3.client('glue', region_name='us-gov-west-1')
        database_name = 'data_broker'
        input_format = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        output_format = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        serde_info = {
            'Name': 'Parquet',
            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
        }
        try:
            response = glue_client.create_table(
                DatabaseName=database_name,
                TableInput={
                    'Name': self.table_ref,
                    'StorageDescriptor': {
                        'Columns': self.columns,
                        'Location': self.table_path,
                        'InputFormat': input_format,
                        'OutputFormat': output_format,
                        'SerdeInfo': serde_info,
                        'Compressed': False,
                        'Parameters': {
                            'classification': 'parquet'
                        }
                    },
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {
                        'EXTERNAL': 'TRUE'
                    }
                }
            )
            print(f"Table '{self.table_name}' created successfully in database '{database_name}'.")
            print(response)

        except glue_client.exceptions.AlreadyExistsException:
            print(f"Table '{self.table_name}' already exists in database '{database_name}'.")
        except Exception as e:
            print(f"Error creating table: {e}")

    def _register_table_hive(self):
        self.spark.sql(rf"""
            CREATE OR REPLACE TABLE {self.table_ref}
            USING DELTA
            LOCATION '{self.hadoop_path}'
        """)
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.table_ref} USING DELTA LOCATION \'{self.table_path}\';")

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

    def to_pandas_df(self):
        return self.dt.to_pyarrow_table().to_pandas()

    def to_polars_df(self):
        return pl.from_arrow(self.dt.to_pyarrow_table())

class DEFCDelta(DeltaModel):
    s3_bucket = 'dti-broker-emr-qat'  # TODO: broker-external, broker-submissions, usas, analytics, etc.
    database = 'int'
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

# Spark - initialize model and schema, hive
# TODO: DUCK DB POPULATION
# TODO: POLARS POPULATION
# AWS Glue? via Boto3?
# MINIO locally
# Focus on streaming data instead of large memory
# Verbose documentation
# Defensive to prevent malicious or data loss
# Validation
# Schema Evolution - Log of history of changes

# What the model looks like base, example table, script used to create table, make schema changes/evolution
# Metastore piece
    # Spark
    # AWS Glue
    # another option hive and glue
# Compare with USAspending Delta Models
# Migrate to Shared Repo

def get_storage_options():
    """ DeltaLake library doesn't use boto3 and doesn't pull the aws creds the same way. """
    aws_creds = get_aws_credentials()
    return {
            "AWS_ACCESS_KEY_ID": aws_creds.access_key,
            "AWS_SECRET_ACCESS_KEY": aws_creds.secret_key,
            "AWS_REGION": "us-gov-west-1",
            "AWS_SESSION_TOKEN": aws_creds.token
    }

if __name__ == "__main__":
    sess = GlobalDB.db().session

    # setup spark
    # spark = setup_spark(hive_url)

    extra_conf = {
        # Config for Delta Lake tables and SQL. Need these to keep Dela table metadata in the metastore
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # See comment below about old date and time values cannot parsed without these
        "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "LEGACY",  # for dates at/before 1900
        "spark.sql.legacy.parquet.int96RebaseModeInWrite": "LEGACY",  # for timestamps at/before 1900
        "spark.sql.jsonGenerator.ignoreNullFields": "false",  # keep nulls in our json
    }

    spark = get_active_spark_session()
    spark_created_by_script = False
    if not spark:
        spark_created_by_script = True
        spark = configure_spark_session(**extra_conf, spark_context=spark)

    # spark = None

    # setup hive connections

    # sqlalchemy
    # hive_engine = create_engine(hive_connection_str)

    # jaydebee
    # driver_class = "org.postgresql.Driver"
    # jar_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'postgresql-42.7.8.jar')
    # conn = jaydebeapi.connect(
    #     driver_class,
    #     hive_url,
    #     [username, password],
    #     jar_file
    # )
    # with conn.cursor() as cursor:
    #     # cursor.execute("SELECT * FROM ;")
    #     pass

    # pyhive
    # conn = hive.Connection(host='your_hive_host', port=10000, username='your_username')
    # cursor = conn.cursor()
    # cursor.execute('SELECT * FROM my_delta_table LIMIT 10')
    # results = cursor.fetchall()
    # print(results)
    # cursor.close()
    # conn.close()

    # get a dataframe from the existing postgres as sample data
    defc_df = pd.read_sql_table(DEFC.__table__.name, sess.connection())

    defc_delta_table = DEFCDelta(spark=spark)

    logger.info('create/initialize the table')
    defc_delta_table.initialize_table()

    logger.info('populating it with data')
    defc_delta_table.merge(defc_df)
    logger.info('Doing it twice to ensure nothing gets duplicated and just updated')
    defc_delta_table.merge(defc_df)

    logger.info('querying it')
    # data = spark.read.csv("s3://your-s3-bucket/input_data.csv", header=True, inferSchema=True)
    # print(data)
    pulled_df_pandas = defc_delta_table.to_pandas_df()
    pulled_df_polars = defc_delta_table.to_polars_df()

    # delta talbe querybuilder - requires registration first
    # defc_aaa = QueryBuilder().execute(f"""
    #     SELECT public_laws
    #     FROM delta.`{defc_delta_table.hadoop_path}`
    #     WHERE code = 'AAA'
    # """).read_all()
    # print(defc_aaa)

    # with hive_engine.connect() as connection:
    #     result = connection.execute("""
    #         SELECT public_laws
    #         FROM `{defc_delta_table.table_ref}`
    #         WHERE code = 'AAA'
    #     """)
    #     print(result)

    results = spark.sql(f"""
        SELECT public_laws
        FROM `{defc_delta_table.table_ref}`
        WHERE code = 'AAA'
    """)
    print(results)

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

    if spark_created_by_script:
        spark.stop()
