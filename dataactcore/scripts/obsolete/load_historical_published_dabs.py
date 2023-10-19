import boto3
import datetime
import logging
import tempfile

import pandas as pd
from pandas import isnull
from sqlalchemy import func

from dataactbroker.helpers.generic_helper import format_internal_tas

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.broker_logging import configure_logging
from dataactcore.models.jobModels import PublishedFilesHistory, Job
from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.lookups import FIELD_TYPE_DICT_ID, FILE_TYPE_DICT, JOB_TYPE_DICT, PUBLISH_STATUS_DICT
from dataactcore.models.stagingModels import (Appropriation, ObjectClassProgramActivity, AwardFinancial,
                                              PublishedAppropriation, PublishedObjectClassProgramActivity,
                                              PublishedAwardFinancial)
from dataactcore.models.userModel import User # noqa
from dataactcore.models.validationModels import FileColumn

from dataactvalidator.health_check import create_app
from dataactcore.utils.loader_utils import insert_dataframe
from dataactvalidator.validation_handlers.validationManager import update_account_nums

logger = logging.getLogger(__name__)

FTI_TABLENAME_DICT = {
    FILE_TYPE_DICT['appropriations']: 'PublishedAppropriation',
    FILE_TYPE_DICT['program_activity']: 'PublishedObjectClassProgramActivity',
    FILE_TYPE_DICT['award_financial']: 'PublishedAwardFinancial'
}
FTI_TABLE_DICT = {
    FILE_TYPE_DICT['appropriations']: PublishedAppropriation,
    FILE_TYPE_DICT['program_activity']: PublishedObjectClassProgramActivity,
    FILE_TYPE_DICT['award_financial']: PublishedAwardFinancial
}
FTI_BASETABLE_DICT = {
    FILE_TYPE_DICT['appropriations']: Appropriation,
    FILE_TYPE_DICT['program_activity']: ObjectClassProgramActivity,
    FILE_TYPE_DICT['award_financial']: AwardFinancial
}


def filenames_by_file_type(file_type_id):
    """ Retrieve the filenames from the PublishedFilesHistory records that are most up-to-date.

        Params:
            file_type_id: DB file type ID for files A, B, or C

        Returns:
            SQLalchemy query
    """
    sess = GlobalDB.db().session
    published_ids = sess.\
        query(func.max(PublishedFilesHistory.published_files_history_id).label('most_recent_id')).\
        filter_by(file_type_id=file_type_id).\
        group_by(PublishedFilesHistory.submission_id).\
        cte('published_ids')

    return sess.query(PublishedFilesHistory.filename, PublishedFilesHistory.submission_id).\
        join(published_ids, published_ids.c.most_recent_id == PublishedFilesHistory.published_files_history_id)


def clean_col(row, col, file_type_id, csv_schema):
    if isnull(row[col]) or not str(row[col]).strip():
        return None

    # Trim
    value = str(row[col]).strip()

    # Replace commas
    if FIELD_TYPE_DICT_ID[csv_schema[col].field_types_id] in ["INT", "DECIMAL", "LONG"]:
        value = value.replace(',', '')

    # Pad to length if necessary
    if csv_schema[col].padded_flag:
        value = str(value).zfill(csv_schema[col].length)

    return value


def insert_from_table(file_type_id, submission_id):
    """ Insert the data from the base staging table into the corresponding Certified table.

        Params:
            file_type_id:  Database file type ID for files A, B, or C
            submission_id: Database ID for the submission being loaded
    """
    sess = GlobalDB.db().session
    logger.info('Copying submission {} data from base table into {}'.format(submission_id,
                                                                            FTI_TABLENAME_DICT[file_type_id]))

    table_type = FTI_BASETABLE_DICT[file_type_id].__table__.name
    column_list = [col.key for col in FTI_BASETABLE_DICT[file_type_id].__table__.columns]
    column_list.remove('created_at')
    column_list.remove('updated_at')
    column_list.remove(table_type + '_id')
    col_string = ", ".join(column_list)

    executed = sess.execute(
        "INSERT INTO published_{} (created_at, updated_at, {}) "
        "SELECT NOW() AS created_at, NOW() AS updated_at, {} "
        "FROM {} "
        "WHERE submission_id={}".format(table_type, col_string, col_string, table_type, submission_id))
    sess.commit()

    logger.info('Loaded {} records into the {} table'.format(executed.rowcount, FTI_TABLENAME_DICT[file_type_id]))


def insert_file(filename, submission_id, file_type_id, csv_schema, long_to_short_dict):
    """ Insert the data from the file into the corresponding Certified table.

        Params:
            filename: filename to load
            submission_id: Database ID for the submission being loaded
            file_type_id:  Database file type ID for files A, B, or C
            csv_schema: Schema built for this filetype's
            long_to_short_dict: Dict to translate long column names to the column names used by the database
    """
    sess = GlobalDB.db().session
    logger.info('Copying "{}" into {} table'.format(filename, FTI_TABLENAME_DICT[file_type_id]))

    # If this is a file in S3, download to a local temp file first then use temp file as local file
    if CONFIG_BROKER['use_aws']:
        (file, tmp_filename) = tempfile.mkstemp()
        s3 = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
        s3.download_file(CONFIG_BROKER['certified_bucket'], filename, tmp_filename)
        filename = tmp_filename

    with open(filename) as file:
        # Get file delimiter and reset reader to start of file
        delim = '|' if file.readline().count('|') != 0 else ','
        file.seek(0)

        # Create dataframe from file
        data = pd.read_csv(file, dtype=str, delimiter=delim)

    # Only use the columns needed for the DB table
    data = data.rename(columns=lambda x: x.lower().strip())
    data = data.rename(index=str, columns=long_to_short_dict)
    # The following columns were added later and need to be accounted for in old data, where they would be blank
    # If DEFC doesn't exist for files B or C, add it and make them all blank
    if 'disaster_emergency_fund_code' not in data and file_type_id in (FILE_TYPE_DICT['program_activity'],
                                                                       FILE_TYPE_DICT['award_financial']):
        data['disaster_emergency_fund_code'] = ''
    # If general_ledger_post_date doesn't exist for file C, add it and make them all blank
    if 'general_ledger_post_date' not in data and file_type_id == FILE_TYPE_DICT['award_financial']:
        data['general_ledger_post_date'] = ''
    data = data[list(csv_schema.keys())]

    # Clean rows
    if len(data.index) > 0:
        for col in long_to_short_dict.values():
            data[col] = data.apply(lambda x: clean_col(x, col, file_type_id, csv_schema), axis=1)

    # Populate columns that aren't in the file
    if len(data.index) > 0:
        data['tas'] = data.apply(lambda x: format_internal_tas(x), axis=1)
    now = datetime.datetime.now()
    data['created_at'] = now
    data['updated_at'] = now
    data['submission_id'] = submission_id
    job = sess.query(Job).filter_by(submission_id=submission_id, file_type_id=file_type_id,
                                    job_type_id=JOB_TYPE_DICT['csv_record_validation']).one()
    data['job_id'] = job.job_id
    data = data.reset_index()
    data['row_number'] = data.index + 2
    data = data.drop(['index'], axis=1)

    # Load dataframe into the DB table
    count = insert_dataframe(data, FTI_TABLE_DICT[file_type_id].__table__.name, sess.connection())
    sess.commit()

    logger.info('Loaded {} records into the {} table'.format(count, FTI_TABLENAME_DICT[file_type_id]))


def main():
    sess = GlobalDB.db().session

    # Fill in long_to_short and short_to_long dicts
    long_to_short_dict = {
        FILE_TYPE_DICT['appropriations']: {'budgetauthorityavailableamounttotal_cpe': 'total_budgetary_resources_cpe',
                                           'budget_authority_available_cpe': 'total_budgetary_resources_cpe'}}
    for col in sess.query(FileColumn.name, FileColumn.name_short, FileColumn.file_id).all():
        if not long_to_short_dict.get(col.file_id):
            long_to_short_dict[col.file_id] = {}
        long_to_short_dict[col.file_id][col.name] = col.name_short

    # Loop through the file types by name
    file_type_names = ['appropriations', 'program_activity', 'award_financial']
    for file_type_name in file_type_names:
        file_type_id = FILE_TYPE_DICT[file_type_name]
        logger.info('Starting to update the {} table'.format(FTI_TABLENAME_DICT[file_type_id]))

        # Load all published files with file_type_id matching the file_type_id
        file_columns = sess.query(FileColumn).filter(FileColumn.file_id == file_type_id).all()
        csv_schema = {f.name_short: f for f in file_columns}
        query = filenames_by_file_type(file_type_id)

        # Loop through the files and load each one individually
        for query_resp in query.all():
            filename = query_resp[0]
            submission_id = query_resp[1]

            # Only add rows to the DB if this submission has not yet been loaded into the certified table
            to_skip = sess.query(FTI_TABLE_DICT[file_type_id]).filter_by(submission_id=submission_id)
            if to_skip.first() is None:
                submission = sess.query(Submission).filter_by(submission_id=submission_id).one()

                if submission.publish_status_id == PUBLISH_STATUS_DICT['published']:
                    # Copy data directly from the table
                    insert_from_table(file_type_id, submission_id)

                else:
                    # Insert data by file
                    insert_file(filename, submission_id, file_type_id, csv_schema, long_to_short_dict[file_type_id])

                    # Populate tas and account_num
                    update_account_nums(FTI_TABLE_DICT[file_type_id], submission_id)
                    sess.commit()
            else:
                logger.info('Skipping file "{}"; Submission {} already loaded'.format(filename, submission_id))


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
