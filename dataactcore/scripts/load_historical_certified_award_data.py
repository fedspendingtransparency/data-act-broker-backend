import boto3
import datetime
import logging
import tempfile

import pandas as pd
from sqlalchemy import func

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.jobModels import CertifiedFilesHistory, Job
from dataactcore.models.jobModels import Submission
from dataactcore.models.validationModels import FileColumn
from dataactcore.models.userModel import User # noqa
from dataactcore.models.lookups import PUBLISH_STATUS_DICT, FILE_TYPE_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT_ID
from dataactcore.models.stagingModels import (AwardFinancialAssistance, AwardProcurement,
                                              CertifiedAwardFinancialAssistance, CertifiedAwardProcurement)
from dataactcore.scripts.load_historical_certified_dabs import clean_col

from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import insert_dataframe

logger = logging.getLogger(__name__)


def copy_certified_submission_award_data(staging_table, certified_table, staging_table_id):
    """ Copy data from the award table to the certified award table for certified DABS submissions.

        Args:
            staging_table: the base table to copy from
            certified_table: the certified table to copy to
            staging_table_id: the primary key of the base table to be ignored when copying over
    """
    staging_table_name = staging_table.__table__.name
    certified_table_name = certified_table.__table__.name
    logger.info('Copying over certified {} data'.format(staging_table_name))
    sess = GlobalDB.db().session

    column_list = [col.key for col in staging_table.__table__.columns]
    column_list.remove('created_at')
    column_list.remove('updated_at')
    column_list.remove(staging_table_id)
    certified_col_string = ', '.join(column_list)
    col_string = ', '.join([col if not col == 'submission_id' else '{}.{}'.format(staging_table_name, col)
                            for col in column_list])

    # Delete the old ones so we don't have conflicts
    clean_sql = """
        DELETE FROM {certified_table}
            USING submission
            WHERE submission.submission_id = {certified_table}.submission_id
                AND publish_status_id = {publish_status}
    """.format(certified_table=certified_table_name, publish_status=PUBLISH_STATUS_DICT['published'])
    sess.execute(clean_sql)

    # Insert all award data from submissions in the certified (not updated) status
    insert_sql = """
        INSERT INTO {certified_table} (created_at, updated_at, {cert_col_string})
        SELECT NOW() AS created_at, NOW() AS updated_at, {col_string}
        FROM {staging_table}
        JOIN submission ON submission.submission_id = {staging_table}.submission_id
        WHERE submission.publish_status_id = {publish_status}
            AND submission.d2_submission IS FALSE
    """.format(staging_table=staging_table_name, certified_table=certified_table_name,
               cert_col_string=certified_col_string, col_string=col_string,
               publish_status=PUBLISH_STATUS_DICT['published'])
    sess.execute(insert_sql)
    sess.commit()
    logger.info('Moved certified {} fields'.format(staging_table_name))


def load_updated_award_data(staging_table, certified_table, file_type_id, daims_to_short, csv_schema):
    """ Load in award data from updated submissions as they were at the latest certification

        Args:
            staging_table: the base table to copy from
            certified_table: the certified table to copy to
            file_type_id: the file type id indicating whether it's procurements or assistance data
            daims_to_short: mapping from DAIMS name to short name to identify the right fields
            csv_schema: mapping from the short name to the appropriate FileColumn object
    """
    staging_table_name = staging_table.__table__.name
    logger.info('Moving updated {} data'.format(staging_table_name))
    sess = GlobalDB.db().session

    # Get a list of all submissions with certified flex fields
    certified_award_subs = sess.query(certified_table.submission_id).distinct().all()

    # We only want to go through updated submissions without award data already loaded
    updated_subs = sess.query(Submission.submission_id).\
        filter(~Submission.submission_id.in_(certified_award_subs),
               Submission.d2_submission.is_(False),
               Submission.publish_status_id == PUBLISH_STATUS_DICT['updated']).all()

    certified_ids = sess. \
        query(func.max(CertifiedFilesHistory.certify_history_id).label('max_cert_id')). \
        filter(CertifiedFilesHistory.submission_id.in_(updated_subs)). \
        group_by(CertifiedFilesHistory.submission_id).cte('certified_ids')

    historical_files = sess.query(CertifiedFilesHistory.filename, CertifiedFilesHistory.file_type_id,
                                  CertifiedFilesHistory.submission_id). \
        join(certified_ids, certified_ids.c.max_cert_id == CertifiedFilesHistory.certify_history_id).\
        filter(CertifiedFilesHistory.file_type_id == file_type_id, CertifiedFilesHistory.filename.isnot(None))

    # Loop through each updated submission
    for historical_file in historical_files:
        filename = historical_file.filename
        submission_id = historical_file.submission_id
        file_type_id = historical_file.file_type_id
        job = sess.query(Job).filter_by(submission_id=submission_id, file_type_id=file_type_id,
                                        job_type_id=JOB_TYPE_DICT['csv_record_validation']).one()

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
            file.seek(0)

            # Create dataframe from file
            data = pd.read_csv(file, dtype=str, delimiter=delim)

        # Only use the columns needed for the DB table
        data = data.rename(columns=lambda x: x.lower().strip())
        data = data.rename(index=str, columns=daims_to_short)
        data = data[list(csv_schema.keys())]

        # Clean rows
        if len(data.index) > 0:
            for col in daims_to_short.values():
                data[col] = data.apply(lambda x: clean_col(x, col, file_type_id, csv_schema), axis=1)

        # Populate columns that aren't in the file
        now = datetime.datetime.now()
        data['created_at'] = now
        data['updated_at'] = now
        data['submission_id'] = submission_id
        data['job_id'] = job.job_id
        data = data.reset_index()
        data['row_number'] = data.index + 2
        data = data.drop(['index'], axis=1)

        logger.info('Moving award data for submission {}, {} file'.format(submission_id,
                                                                          FILE_TYPE_DICT_ID[file_type_id]))

        # Process and insert the data
        insert_dataframe(data, certified_table.__table__.name, sess.connection())
        sess.commit()

    logger.info('Moved updated {} data'.format(staging_table_name))


def main():
    """ Load award data for certified submissions that haven't been loaded into the certified award tables. """
    sess = GlobalDB.db().session
    aw_data_map = {
        'award_procurement': {
            'staging_table': AwardProcurement,
            'certified_table': CertifiedAwardProcurement,
            'id': 'award_procurement_id',
            'file_type_id': FILE_TYPE_DICT['award_procurement']
        },
        'award_financial_assistance': {
            'staging_table': AwardFinancialAssistance,
            'certified_table': CertifiedAwardFinancialAssistance,
            'id': 'award_financial_assistance_id',
            'file_type_id': FILE_TYPE_DICT['award']
        }
    }

    for award_type, award_dict in aw_data_map.items():
        # Load all certified files with file_type_id matching the file_type_id
        file_columns = sess.query(FileColumn).filter(FileColumn.file_id == award_dict['file_type_id']).all()
        daims_to_short = {f.daims_name.lower().strip(): f.name_short for f in file_columns}
        csv_schema = {f.name_short: f for f in file_columns}

        copy_certified_submission_award_data(award_dict['staging_table'], award_dict['certified_table'],
                                             award_dict['id'])
        load_updated_award_data(award_dict['staging_table'], award_dict['certified_table'], award_dict['file_type_id'],
                                daims_to_short, csv_schema)


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
