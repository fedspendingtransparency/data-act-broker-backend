import logging
import pandas as pd

from dataactbroker.helpers.spark_helper import configure_spark_session, get_active_spark_session
from dataactcore.config import CONFIG_BROKER, CONFIG_DB
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

    print('use_aws', CONFIG_BROKER["use_aws"])

    # databases = spark.catalog.listDatabases()
    # for db in databases:
    #     print(f"Tables in database: {db.name}")
    #     spark.sql(f"SHOW TABLES IN {db.name}").show()

    results = spark.sql(f"""
        SELECT *
        FROM {defc_delta_table.table_ref}
        WHERE code = 'AAA'
    """).show()

    if spark_created_by_command:
        spark.stop()
