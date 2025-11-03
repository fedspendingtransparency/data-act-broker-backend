import logging
import pandas as pd

from dataactbroker.helpers.spark_helper import configure_spark_session, get_active_spark_session
from dataactcore.emr.models.reference import DEFCDelta
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import DEFC

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    sess = GlobalDB.db().session

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
    spark_created_by_command = False
    if not spark:
        spark_created_by_command = True
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

    # TODO: Merging with data already in it -> "Metadata changed since last commit...."?
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


    # databases = spark.catalog.listDatabases()
    # for db in databases:
    #     print(f"Tables in database: {db.name}")
    #     spark.sql(f"SHOW TABLES IN {db.name}").show()

    results = spark.sql(f"""
        SELECT *
        FROM {defc_delta_table.table_ref}
        WHERE code = 'AAA'
    """).show()

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

    if spark_created_by_command:
        spark.stop()
