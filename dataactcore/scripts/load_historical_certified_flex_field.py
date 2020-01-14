import csv
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
from dataactcore.models.jobModels import Submission
from dataactcore.models.userModel import User # noqa
from dataactcore.models.lookups import PUBLISH_STATUS_DICT, FILE_TYPE_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT_ID
from dataactcore.models.stagingModels import FlexField, CertifiedFlexField

from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import insert_dataframe

logger = logging.getLogger(__name__)

FILE_LIST = [FILE_TYPE_DICT['appropriations'], FILE_TYPE_DICT['program_activity'], FILE_TYPE_DICT['award_financial']]


def copy_certified_submission_flex_fields():
    """ Copy flex fields from the flex_field table to the certified_flex_field table for certified DABS submissions. """
    logger.info('Moving certified flex fields')
    sess = GlobalDB.db().session

    column_list = [col.key for col in FlexField.__table__.columns]
    column_list.remove('created_at')
    column_list.remove('updated_at')
    column_list.remove('flex_field_id')
    certified_col_string = ', '.join(column_list)
    col_string = ', '.join([col if not col == 'submission_id' else 'flex_field.' + col for col in column_list])

    # Delete the old ones so we don't have conflicts
    sess.execute(
        """DELETE FROM certified_flex_field
            USING submission
            WHERE submission.submission_id = certified_flex_field.submission_id
                AND publish_status_id = {}
        """.format(PUBLISH_STATUS_DICT['published']))

    # Insert all flex fields from submissions in the certified (not updated) status
    sess.execute(
        """INSERT INTO certified_flex_field (created_at, updated_at, {})
            SELECT NOW() AS created_at, NOW() AS updated_at, {}
            FROM flex_field
            JOIN submission ON submission.submission_id = flex_field.submission_id
            WHERE submission.publish_status_id = {}
                AND submission.d2_submission IS FALSE
        """.format(certified_col_string, col_string, PUBLISH_STATUS_DICT['published']))
    sess.commit()
    logger.info('Moved certified flex fields')


def clean_col(datum):
    if isnull(datum) or not str(datum).strip():
        return None

    # Trim
    return str(datum).strip()


def process_flex_data(data, flex_headers, submission_id, job_id, file_type_id):
    """ Process the file that contains flex fields and insert all flex cells into the certified table

        Args:
            data: The pandas dataframe containing the file
            flex_headers: The flex fields contained in this file
            submission_id: The ID associated with the submission this file comes from
            job_id: The ID associated with the job this file comes from
            file_type_id: The ID of the file type that this is
    """

    # Only use the flex columns
    data = data.rename(columns=lambda x: x.lower().strip())
    data = data[list(flex_headers)]

    if len(data.index) > 0:
        data = data.applymap(clean_col)

    # Populate row number, adding 2 to the index because the first row is always row 2 but index starts at 0
    data = data.reset_index()
    data['row_number'] = data.index + 2
    data = data.drop(['index'], axis=1)

    # Split each flex field into its own row with both content and headers while keeping the row number
    new_df = pd.melt(data, id_vars=['row_number'], value_vars=flex_headers, var_name='header', value_name='cell')

    # Filling in all the shared data for these flex fields
    now = datetime.datetime.now()
    new_df['created_at'] = now
    new_df['updated_at'] = now
    new_df['job_id'] = job_id
    new_df['submission_id'] = submission_id
    new_df['file_type_id'] = file_type_id

    return new_df


def load_updated_flex_fields():
    """ Load in flex fields from updated submissions as they were at the latest certification """
    logger.info('Moving updated flex fields')
    sess = GlobalDB.db().session

    # Get a list of all submissions with certified flex fields
    certified_flex_subs = sess.query(CertifiedFlexField.submission_id).distinct().all()

    # We only want to go through updated submissions without flex fields already loaded
    updated_subs = sess.query(Submission.submission_id).\
        filter(~Submission.submission_id.in_(certified_flex_subs),
               Submission.d2_submission.is_(False),
               Submission.publish_status_id == PUBLISH_STATUS_DICT['updated']).all()

    certified_ids = sess. \
        query(func.max(CertifiedFilesHistory.certify_history_id).label('max_cert_id')). \
        filter(CertifiedFilesHistory.submission_id.in_(updated_subs)). \
        group_by(CertifiedFilesHistory.submission_id).cte('certified_ids')

    historical_files = sess.query(CertifiedFilesHistory.filename, CertifiedFilesHistory.file_type_id,
                                  CertifiedFilesHistory.submission_id). \
        join(certified_ids, certified_ids.c.max_cert_id == CertifiedFilesHistory.certify_history_id).\
        filter(CertifiedFilesHistory.file_type_id.in_(FILE_LIST))

    # Loop through each updated submission
    for historical_file in historical_files:
        filename = historical_file.filename
        submission_id = historical_file.submission_id
        file_type_id = historical_file.file_type_id

        # If this is a file in S3, download to a local temp file first then use temp file as local file
        if CONFIG_BROKER['use_aws']:
            (file, tmp_filename) = tempfile.mkstemp()
            s3 = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
            s3.download_file(CONFIG_BROKER['certified_bucket'], filename, tmp_filename)
            filename = tmp_filename

        with open(filename) as file:
            # Get file delimiter, get an array of the header row, and reset reader to start of file
            header_line = file.readline()
            delim = '|' if header_line.count('|') != 0 else ','
            header_row = next(csv.reader([header_line], quotechar='"', dialect='excel', delimiter=delim))
            file.seek(0)

            flex_list = [header.lower() for header in header_row if header.lower().startswith('flex_')]

            # If there are no flex fields, just ignore this file, no need to go through it
            if len(flex_list) == 0:
                continue

            # Create dataframe from file
            data = pd.read_csv(file, dtype=str, delimiter=delim)

        logger.info('Moving flex fields for submission {}, {} file'.format(submission_id,
                                                                           FILE_TYPE_DICT_ID[file_type_id]))

        # Getting the job so we can get the ID
        job = sess.query(Job).filter_by(submission_id=submission_id, file_type_id=file_type_id,
                                        job_type_id=JOB_TYPE_DICT['csv_record_validation']).one()

        # Process and insert the data
        flex_data = process_flex_data(data, flex_list, submission_id, job.job_id, file_type_id)
        insert_dataframe(flex_data, CertifiedFlexField.__table__.name, sess.connection())
        sess.commit()

    logger.info('Moved updated flex fields')


def main():
    """ Load flex fields for certified submissions that haven't been loaded into the certified flex fields table. """

    copy_certified_submission_flex_fields()
    load_updated_flex_fields()


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
