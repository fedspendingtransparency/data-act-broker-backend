import logging
import argparse
from sqlalchemy import and_, or_
import pandas as pd

from dataactcore.interfaces.db import GlobalDB
from dataactcore.broker_logging import configure_logging
from dataactvalidator.health_check import create_app

from dataactbroker.helpers.generic_helper import batch
from dataactcore.utils.sam_recipient import update_existing_recipients, update_sam_recipient, LOAD_BATCH_SIZE
from dataactcore.models.domainModels import SAMRecipient

logger = logging.getLogger(__name__)


def backfill_uei_via_entity_api(sess, table):
    """Backfill any extraneous data (ex. uei) missing from V1 data that wasn't updated by V2

    Args:
        sess: database connection
        table: table to backfill
    """
    duns_to_update = (
        sess.query(table.awardee_or_recipient_uniqu)
        .filter(
            or_(
                SAMRecipient.uei.is_(None),
                and_(SAMRecipient.ultimate_parent_unique_ide.isnot(None), SAMRecipient.ultimate_parent_uei.is_(None)),
            )
        )
        .all()
    )
    for duns_batch in batch(duns_to_update, LOAD_BATCH_SIZE):
        df = pd.DataFrame(columns=["awardee_or_recipient_uniqu"])
        df = df.append(duns_batch)
        df = update_existing_recipients(df)
        df = df[["awardee_or_recipient_uniqu", "uei", "ultimate_parent_uei"]]
        update_sam_recipient(sess, df, table_name=table.__table__.name)


def backfill_uei_crosswalk(sess, table_name):
    """Backfill any extraneous data (ex. uei) missing from V1 data that wasn't updated by V2

    Args:
        sess: database connection
        table_name: table to backfill
    """
    blank_uei_query = """
        SELECT awardee_or_recipient_uniqu
        FROM {table_name}
        WHERE uei IS NULL;
    """.format(
        table_name=table_name
    )
    duns_to_update = [row["awardee_or_recipient_uniqu"] for row in sess.execute(blank_uei_query).fetchall()]
    for duns_batch in batch(duns_to_update, LOAD_BATCH_SIZE):
        df = pd.DataFrame()
        df["awardee_or_recipient_uniqu"] = duns_batch
        df = update_existing_recipients(df, api="iqaas")
        df = df[["awardee_or_recipient_uniqu", "uei"]]
        update_sam_recipient(sess, df, table_name=table_name)


if __name__ == "__main__":
    configure_logging()

    parser = argparse.ArgumentParser(description="Get data from SAM and backfill uei")
    parser.add_argument(
        "-m",
        "--method",
        choices=["sam_recipient", "crosswalk"],
        default="crosswalk",
        help="Select method of backfilling (sam_recipient table, uei crosswalk table)",
    )
    parser.add_argument(
        "-ct", "--crosswalk_table", default="uei-crosswalk", help="Name of the crosswalk table to backfill"
    )
    args = parser.parse_args()

    method = args.method
    crosswalk_table = args.crosswalk_table

    with create_app().app_context():
        sess = GlobalDB.db().session

        affected = 0

        if method == "sam_recipient":
            logger.info("Backfilling empty uei and ultimate_parent_uei in the SAMRecipient table using the entity API.")
            backfill_uei_via_entity_api(sess, SAMRecipient)
        else:
            logger.info("Backfilling {} using the IQaaS API.".format(crosswalk_table))
            backfill_uei_crosswalk(sess, table_name=crosswalk_table)
        logger.info("Backfill completed")

        sess.close()
