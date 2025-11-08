import logging
import pandas as pd

from brus_backend_common.helpers.spark_helper import SparkScriptSession
from brus_backend_common.models.reference import DEFCDelta
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import DEFC

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # get a dataframe from the existing postgres as sample data
    sess = GlobalDB.db().session
    defc_df = pd.read_sql_table(DEFC.__table__.name, sess.connection())

    with SparkScriptSession() as spark:
        defc_delta_table = DEFCDelta(spark=spark)

        logger.info("create/initialize the table")
        defc_delta_table.initialize()

        # TODO: Merging with data already in it -> "Metadata changed since last commit...."?
        logger.info("populating it with data")
        defc_delta_table.merge(defc_df)
        logger.info("Doing it twice to ensure nothing gets duplicated and just updated")
        defc_delta_table.merge(defc_df)

        # databases = spark.catalog.listDatabases()
        # for db in databases:
        #     print(f"Tables in database: {db.name}")
        #     spark.sql(f"SHOW TABLES IN {db.name}").show()

        results = spark.sql(
            f"""
            SELECT *
            FROM {defc_delta_table.table_ref}
            WHERE code = 'AAA'
        """
        ).show()