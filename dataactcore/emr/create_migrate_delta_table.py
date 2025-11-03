import argparse
import logging

from dataactcore.emr.models import DELTA_MODELS
from dataactbroker.helpers.spark_helper import configure_spark_session, get_active_spark_session

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create or migrate delta tables")
    parser.add_argument(
        "--table", "-t",
        type=str,
        required=True,
        help="The destination Delta Table to write the data",
        choices=list(DELTA_MODELS.keys()),
    )
    behavior = parser.add_mutually_exclusive_group(required=False)
    behavior.add_argument(
        "--recreate", "-r",
        type=bool,
        action="store_true",
        required=False,
        help="If the table already exists, recreate it as a way of updating it to the latest. "
             "Note: this will obviously remove the table in its current state.",
    )
    behavior.add_argument(
        "--migrate", "-m",
        type=int,
        required=False,
        help="If the table already exists, run a migration chain of spark scripts to update it. "
             "Must be negative values pulling from the end of the history "
             "(0 = all, -1 = last, -2 = 2nd from last, last). ",
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
    table_exists = model.exists()
    recreate = args.recreate
    migrate = args.migrate
    if migrate and not table_exists:
        raise ValueError('Migration provided but table doesn\'t exist.')
    elif migrate > 0:
        raise ValueError('Migration provided but not a negative value.')
    elif -1 * migrate > len(model.migrations):
        raise ValueError('Migration exceeds the amount of table migrations available.')

    if table_exists and migrate:
        model.migrate(migrate)
    else:
        model.initialize(recreate=recreate)

    if spark_created_by_command:
        spark.stop()
