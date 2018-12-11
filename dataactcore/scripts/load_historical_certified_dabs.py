import boto3
import logging
import tempfile

import numpy as np
import pandas as pd
from sqlalchemy import func

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.jobModels import CertifiedFilesHistory, Job
from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.lookups import FILE_TYPE_DICT, JOB_TYPE_DICT
from dataactcore.models.stagingModels import CertifiedAppropriation
from dataactcore.models.userModel import User # noqa
from dataactcore.models.validationModels import FileColumn

from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import insert_dataframe
from dataactvalidator.validation_handlers.validationManager import update_tas_ids

logger = logging.getLogger(__name__)

FILE_TYPE_ID_TABLENAME_DICT = {
    FILE_TYPE_DICT['appropriations']: 'CertifiedAppropriation',
    FILE_TYPE_DICT['program_activity']: 'CertifiedObjectClassProgramActivity',
    FILE_TYPE_DICT['award_financial']: 'CertifiedAwardFinancial',
}


def filenames_by_file_type(file_type_id):
    """ Retrieve the filenames from the CertifiedFilesHistory records that are most up-to-date.

        Params:
            file_type_id: DB file type ID for files A, B, or C

        Returns:
            SQLalchemy query
    """
    sess = GlobalDB.db().session
    certified_ids = sess.\
        query(func.max(CertifiedFilesHistory.certified_files_history_id).label('most_recent_id')).\
        filter_by(file_type_id=file_type_id).\
        group_by(CertifiedFilesHistory.submission_id).\
        cte('most_recent_id')

    return sess.query(CertifiedFilesHistory.filename, CertifiedFilesHistory.submission_id).\
        join(certified_ids, certified_ids.c.most_recent_id == CertifiedFilesHistory.certified_files_history_id)


def insert_file(filename, submission_id, file_type_id, fields, long_to_short_dict):
    """ Insert the data from the file into the corresponding Certified table.

        Params:
            filename: filename to load
            submission_id: Database ID for the submission being loaded
            file_type_id:  Database file type ID for files A, B, or C
            fields: Columns to pull from the file into the database
            long_to_short_dict: Dict to translate long column names to the column names used by the database
    """
    sess = GlobalDB.db().session
    logger.info('Copying "{}" into {} table'.format(filename, FILE_TYPE_ID_TABLENAME_DICT[file_type_id]))

    # If this is a file in S3, download to a local temp file first then use temp file as local file
    if CONFIG_BROKER['use_aws']:
        (file, tmp_filename) = tempfile.mkstemp()
        s3 = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
        s3.download_file(CONFIG_BROKER['aws_bucket'], filename, tmp_filename)
        filename = tmp_filename

    # Create dataframe from file
    with open(filename) as file:
        data = pd.read_csv(file, dtype=str)

    # Only use the rows needed for the DB table
    data = data.rename(columns=lambda x: x.lower().strip())
    data = data.rename(index=str, columns=long_to_short_dict)
    data = data[fields]

    # Populate submission and job ID columns, and clean the empty rows
    data['submission_id'] = submission_id
    job = sess.query(Job).filter_by(submission_id=submission_id, file_type_id=file_type_id,
                                    job_type_id=JOB_TYPE_DICT['csv_record_validation']).one()
    data['job_id'] = job.job_id
    data = data.replace(np.nan, '', regex=True)

    # Load dataframe into the DB table
    count = insert_dataframe(data, FILE_TYPE_ID_TABLENAME_DICT[file_type_id], sess.connection())
    sess.commit()

    logger.info('Loaded {} records into the {} table'.format(count, FILE_TYPE_ID_TABLENAME_DICT[file_type_id]))


def main():
    sess = GlobalDB.db().session

    # Fill in long_to_short and short_to_long dicts
    long_to_short_dict = {}
    for col in sess.query(FileColumn.name, FileColumn.name_short, FileColumn.file_id).all():
        if not long_to_short_dict.get(col.file_id):
            long_to_short_dict[col.file_id] = {}
        long_to_short_dict[col.file_id][col.name] = col.name_short

    # Loop through the file types by name
    file_type_names = ['appropriations', 'program_activity', 'award_financial']
    for file_type_name in file_type_names:
        file_type_id = FILE_TYPE_DICT[file_type_name]
        logger.info('Starting to update the {} table'.format(FILE_TYPE_ID_TABLENAME_DICT[file_type_id]))

        # Load all certified files with file_type_id matching the file_type_id
        file_columns = sess.query(FileColumn).filter(FileColumn.file_id == file_type_id).all()
        # csv_schema = {f.name_short: f for f in file_columns}
        fields = [f.name_short for f in file_columns]
        query = filenames_by_file_type(file_type_id)

        # Loop through the files and load each one individually
        for query_resp in query.all():
            filename = query_resp[0]
            submission_id = query_resp[1]
            insert_file(filename, submission_id, file_type_id, fields, long_to_short_dict[file_type_id])

            if file_type_name == 'appropriations':
                update_tas_ids(CertifiedAppropriation, submission_id)


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
