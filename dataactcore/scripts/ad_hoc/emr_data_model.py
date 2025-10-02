from abc import ABC, abstractmethod
import pandas as pd
from datetime import date, datetime

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import DEFC

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, ArrayType, BooleanType, DateType,
                               DecimalType, NullType, TimestampType)
from delta.tables import DeltaTable
from delta import *

import subprocess
import sys
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

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

class DeltaModel(DeltaTable):
    def __init__(self, spark):
        self.spark = spark
        super().__init__(spark, self.table_path)

    @property
    def s3_bucket(self):
        env = 'qat'
        return f'dti-broker-emr-{env}'

    @property
    def table_path(self):
        return f's3://{self.s3_bucket}/{self.bucket}/{self.schema}/{self.table_name}'

    @property
    @abstractmethod
    def schema(self):
        pass

    @property
    @abstractmethod
    def table_name(self):
        pass

    def initialize_table(self):
        return self.createIfNotExists(self.spark)\
            .tableName(self.table_name)\
            .location(self.table_path)\
            .addColumns(self.structure)\
            .execute()

class DEFCDelta(DeltaModel):
    @property
    def bucket(self):
        return 'reference'

    @property
    def schema(self):
        return 'int'

    @property
    def table_name(self):
        return 'defc'

    # defc_id = Column(Integer)
    # code = Column(Text, nullable=False, unique=True)
    # public_laws = Column(Array(Text))
    # public_law_short_titles = Column(Array(Text))
    # group = Column(Text)
    # urls = Column(Array(Text))
    # is_valid = Column(Boolean, nullable=False)
    # earliest_pl_action_date = Column(DateTime)

    @property
    def unique_constraints(self):
        return [('code')]

    @property
    def structure(self):
        return StructType([
            StructField("defc_id", IntegerType(), True),
            StructField("code", StringType(), False),
            StructField("public_laws", ArrayType(StringType()), True),
            StructField("group", StringType(), True),
            StructField("urls", ArrayType(StringType()), False),
            StructField("is_valid", BooleanType(), True),
            StructField("earliest_pl_action_date", TimestampType(), True),
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


# TODO: DUCK DB POPULATION

# TODO: POLARS POPULATION

if __name__ == "__main__":
    spark = setup_spark()

    # setup hive connection with SQLAlchemy
    # engine = create_engine('hive://localhost:10000/default')

    # get a dataframe from the existing postgres as sample data
    sess = GlobalDB.db().session
    defc_df = pd.read_sql_table(DEFC.__table__.name, sess.connection())

    # print('create delta table')
    defc_delta_table = DEFCDelta(spark=spark).initialize_table()

    # print('populating it with two rows')
    # employees_data = spark.createDataFrame([(101, "Alice", "alice@example.com", "IT")],
    #                                        ["id", "name", "email", "department"])
    # df = spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
    #
    # print('querying it')
    # data = spark.read.csv("s3://your-s3-bucket/input_data.csv", header=True, inferSchema=True)
    #
    #
    # print('updating a value')
    # deltaTable.update("id = 1", {"value": "'new_value'"})
    # deltaTable = DeltaTable.replace(spark).tableName("testTable").addColumns(df.schema).execute()
    #
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