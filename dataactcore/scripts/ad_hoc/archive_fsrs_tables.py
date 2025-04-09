import boto3
import logging
import os
import numpy as np
import re

from dataactbroker.handlers.submission_handler import delete_submission_files

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.jobModels import Submission
from dataactcore.broker_logging import configure_logging
from dataactcore.models.fsrs import FSRSProcurement, FSRSSubcontract, FSRSGrant, FSRSSubgrant

from dataactvalidator.filestreaming.csv_selection import write_stream_query
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def main():
    """ Archive the old FSRS tables """
    sess = GlobalDB.db().session

    logger.info('Starting FSRS archive')

    for fsrs_table in [FSRSProcurement, FSRSSubcontract, FSRSGrant, FSRSSubgrant]:
        local_file = os.path.join(os.getcwd(), f'{fsrs_table.__table__.name}_archive.csv')
        fsrs_query = sess.query(fsrs_table)
        write_stream_query(sess, fsrs_query, local_file, local_file, CONFIG_BROKER['local'], generate_headers=True,
                           generate_string=True, bucket='dti-da-data-archive-prod')

    subaward_fsrs_old_query = """
        SELECT *
        FROM subaward_fsrs_old
    """
    local_file = os.path.join(os.getcwd(), 'subaward_fsrs_old_archive.csv')
    write_stream_query(sess, subaward_fsrs_old_query, local_file, local_file, CONFIG_BROKER['local'], generate_headers=True,
                       generate_string=False, bucket='dti-da-data-archive-prod')
    logger.info('Completed SQL query, file written')

    logger.info('FSRS archive complete')


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
