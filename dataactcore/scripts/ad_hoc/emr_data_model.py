from abc import ABC, abstractmethod
import pandas as pd

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import DEFC

# AWSGlue
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from delta import *

# pyHive
import subprocess
import sys
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

class ColumnType(ABC):
    # @abstractmethod
    pass

class Text(ColumnType):
    pass

class Integer(ColumnType):
    pass

class Double(ColumnType):
    pass

class Boolean(ColumnType):
    pass

class Array(ColumnType):
    def __init__(self, type):
        self.item_type = type

class Date(ColumnType):
    pass

class DateTime(ColumnType):
    pass

class Column(ABC):
    @abstractmethod

    def __init__(self, column_type: ColumnType, default: bool, nullable: bool, unique: bool):
        self.column_type = column_type


class DeltaModel(ABC):
    def __init__(self, spark):
        self.spark = spark

    @property
    def bucket(self):
        env = 'qat'
        return f'dti-broker-emr-{env}'

    @property
    def table_path(self):
        return f's3://{self.bucket}/{self.schema}/{self.table_name}'

    @property
    @abstractmethod
    def table_name(self):
        pass

    @abstractmethod
    def to_dataframe(self):
        pass

    def create(self):
        self.spark.createDataFrame()

    def append(self, df):
        df.write.format("delta").mode("append").option("mergeSchema", "true").save(self.table_path)

    def overwrite(self, df):
        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(self.table_path)


class DEFCDelta(DeltaModel):
    @property
    def table_name(self):
        return 'defc'

    defc_id = Column(Integer)
    code = Column(Text, nullable=False, unique=True)
    public_laws = Column(Array(Text))
    public_law_short_titles = Column(Array(Text))
    group = Column(Text)
    urls = Column(Array(Text))
    is_valid = Column(Boolean, nullable=False)
    earliest_pl_action_date = Column(DateTime)


def setup_spark():
    # Initialize SparkSession for AWS Glue
    spark = SparkSession.builder \
        .appName("GlueDeltaLakeJob") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark


# if __name__ == "__main__":
    # spark = setup_spark()

    # setup hive connection with SQLAlchemy
    # engine = create_engine('hive://localhost:10000/default')

    # get a dataframe from the existing postgres as sample data
    sess = GlobalDB.db().session
    defc_df = pd.read_sql_table(DEFC.__table__.name, sess.connection())
    print(defc_df)

    # print('create delta table')

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