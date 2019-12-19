import boto3
import datetime
import logging
import tempfile

import pandas as pd
from pandas import isnull
from sqlalchemy import func

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.jobModels import CertifiedFilesHistory, Job
from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.lookups import PUBLISH_STATUS_DICT
from dataactcore.models.stagingModels import FlexField

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def copy_certified_submission_flex_fields():
    """ Copy flex fields from the flex_field table to the certified_flex_field table for certified DABS submissions. """
    logger.info('Moving certified flex fields')
    sess = GlobalDB.db().session

    column_list = [col.key for col in FlexField.__table__.columns]
    column_list.remove('created_at')
    column_list.remove('updated_at')
    column_list.remove('flex_field_id')
    old_col_string = ', '.join(column_list)
    new_col_string = ', '.join([col if not col == 'submission_id' else 'flex_field.' + col for col in column_list])

    # Delete the old ones so we don't have conflicts
    sess.execute("""
        DELETE FROM certified_flex_field
        USING submission
        WHERE submission.submission_id = certified_flex_field.submission_id
            AND publish_status_id = {}
        """.format(PUBLISH_STATUS_DICT['published']))

    sess.execute(
        "INSERT INTO certified_flex_field (created_at, updated_at, {}) "
        "SELECT NOW() AS created_at, NOW() AS updated_at, {} "
        "FROM flex_field "
        "JOIN submission ON submission.submission_id = flex_field.submission_id "
        "WHERE submission.publish_status_id = {}".format(old_col_string, new_col_string,
                                                         PUBLISH_STATUS_DICT['published']))
    sess.commit()
    logger.info('Moved certified flex fields')


def main():
    """ Load flex fields for certified submissions that haven't been loaded into the certified flex fields table. """

    copy_certified_submission_flex_fields()


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
