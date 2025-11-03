import argparse
import logging

from dataactcore.emr.models import DELTA_MODELS
from dataactbroker.helpers.spark_helper import configure_spark_session, get_active_spark_session

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate a delta table with its designated query.")
    parser.add_argument(
        "--table", "-t",
        type=str,
        required=True,
        help="The destination Delta Table to write the data",
        choices=list(DELTA_MODELS.keys()),
    )
    parser.add_argument(
        "--incremental", "-i",
        type=str,
        required=False,
        action="store_true",
        help="Whether to incrementally add to the table or repopulate entirely",
    )
    args = parser.parse_args()

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

    model = DELTA_MODELS[args.table](spark=spark)
    incremental = args.incremental
    table_exists = model.exists()
    if not table_exists:
        raise ValueError('Table doesn\'t exist. Use create_migrate_delta_table beforehand.')

    if incremental:
        model.increment()
    else:
        model.repopulate()

    if spark_created_by_command:
        spark.stop()
