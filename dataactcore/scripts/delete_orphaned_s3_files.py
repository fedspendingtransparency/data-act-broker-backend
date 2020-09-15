import boto3
import logging
import numpy as np
import re

from dataactbroker.handlers.submission_handler import delete_submission_files

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.jobModels import Submission
from dataactcore.logging import configure_logging

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def main():
    """ Delete all submission files in S3 that no longer connect to a submission that exists in the DB """
    logger.info({
        'message': 'Deleting orphaned submission files',
        'message_type': 'BrokerInfo'
    })
    s3 = boto3.resource('s3', region_name=CONFIG_BROKER['aws_region'])
    bucket = s3.Bucket(CONFIG_BROKER['aws_bucket'])

    # get the submission IDs from S3
    submission_ids = []
    for bucket_object in bucket.objects.all():
        if re.match('^\d+/.*', bucket_object.key):
            submission_ids.append(bucket_object.key.split('/')[0])
    submission_ids = list(set(submission_ids))

    # get the submission IDs from the database
    database_submission_ids = []
    sess = GlobalDB.db().session
    sub_query = sess.query(Submission.submission_id)
    for sub in sub_query.all():
        database_submission_ids.append(str(sub.submission_id))

    # Get only the submission IDs from S3 that aren't in the database
    orphaned_subs = np.setdiff1d(submission_ids, database_submission_ids)

    for sub in orphaned_subs:
        delete_submission_files(sess, sub)

    logger.info({
        'message': 'Finished deleting orphaned submission files',
        'message_type': 'BrokerInfo'
    })


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
