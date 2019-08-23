import logging
import os
import re
import numpy as np
import pandas as pd

from sqlalchemy import func

from dataactbroker.helpers.uri_helper import RetrieveFileFromUri

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.errorModels import ErrorMetadata, CertifiedErrorMetadata
from dataactcore.models.jobModels import Job, Submission, CertifyHistory, CertifiedFilesHistory
from dataactcore.models.lookups import (PUBLISH_STATUS_DICT, RULE_SEVERITY_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT,
                                        ERROR_TYPE_DICT)

from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import insert_dataframe

from dataactcore.models.validationModels import RuleSeverity  # noqa
from dataactcore.models.userModel import User  # noqa

logger = logging.getLogger(__name__)


def move_certified_error_metadata(sess):
    """ Simply move the error metadata for certified submissions since that one is valid.

        Args:
            sess: connection to database
    """
    logger.info('Moving certified error metadata')
    # Get a list of all jobs for certified submissions that aren't FABS
    certified_job_list = sess.query(Job.job_id).join(Submission, Job.submission_id == Submission.submission_id).\
        filter(Submission.d2_submission.is_(False), Submission.publish_status_id == PUBLISH_STATUS_DICT['published']).\
        all()

    # Delete all current certified entries to prevent duplicates
    sess.query(CertifiedErrorMetadata).filter(CertifiedErrorMetadata.job_id.in_(certified_job_list)).\
        delete(synchronize_session=False)

    # Create dict of error metadata
    error_metadata_objects = sess.query(ErrorMetadata).filter(ErrorMetadata.job_id.in_(certified_job_list)).all()
    error_metadata_list = []
    for obj in error_metadata_objects:
        tmp_obj = obj.__dict__
        tmp_obj.pop('_sa_instance_state')
        tmp_obj.pop('created_at')
        tmp_obj.pop('updated_at')
        tmp_obj.pop('error_metadata_id')
        error_metadata_list.append(obj.__dict__)

    # Save all the objects in the certified error metadata table
    sess.bulk_save_objects([CertifiedErrorMetadata(**error_metadata) for error_metadata in error_metadata_list])
    sess.commit()
    logger.info('Certified error metadata moved')


def convert_file_type_to_int(row, col_type):
    """ Converting the string of a file type to its corresponding integer. Helper function for dataframe lambda

        Args:
            row: the row from the dataframe
            col_type: the row from the dataframe

        Returns:
            None if the row doesn't have a file type in the provided column, the integer corresponding to the name
            of the file otherwise.
    """
    if not row[col_type]:
        return None
    return FILE_TYPE_DICT[row[col_type]]


def derive_error_type_id(row):
    """ Finding the type of the error (warning). Helper function for dataframe lambda

        Args:
            row: the row from the dataframe

        Returns:
            The integer corresponding to the type of warning.
    """
    # If it's a maximum length error this will always be part of the message
    if 'Value was longer than maximum length' in row['error_message']:
        return ERROR_TYPE_DICT['length_error']
    # If it's not a length issue, it has to be a rule in order for it to be a warning
    return ERROR_TYPE_DICT['rule_failed']


def move_updated_error_metadata(sess):
    """ Moving the last certified error metadata for updated submissions.

        Args:
            sess: connection to database
    """
    logger.info('Moving updated error metadata')
    # Get a list of all jobs for updated submissions (these can't be FABS but we'll filter in case there's a bug)
    updated_job_list = sess.query(Job.job_id).join(Submission, Job.submission_id == Submission.submission_id). \
        filter(Submission.d2_submission.is_(False), Submission.publish_status_id == PUBLISH_STATUS_DICT['updated']). \
        all()

    # Delete all current updated entries to prevent duplicates
    sess.query(CertifiedErrorMetadata).filter(CertifiedErrorMetadata.job_id.in_(updated_job_list)). \
        delete(synchronize_session=False)

    # Create a CTE of the max certify history IDs for updated submissions (DABS only)
    max_certify_history = sess.query(func.max(CertifyHistory.certify_history_id).label('max_certify_id'),
                                     CertifyHistory.submission_id.label('submission_id')).\
        join(Submission, CertifyHistory.submission_id == Submission.submission_id).\
        filter(Submission.publish_status_id == PUBLISH_STATUS_DICT['updated'], Submission.d2_submission.is_(False)).\
        group_by(CertifyHistory.submission_id).cte('max_certify_history')

    # Get the certify history associated with all of the warning files
    certify_history_list = sess.query(CertifiedFilesHistory.certify_history_id, CertifiedFilesHistory.submission_id,
                                      CertifiedFilesHistory.warning_filename).\
        join(max_certify_history, max_certify_history.c.max_certify_id == CertifiedFilesHistory.certify_history_id).\
        filter(CertifiedFilesHistory.warning_filename.isnot(None)).order_by(CertifiedFilesHistory.submission_id).\
        distinct()

    # Creating temporary error table and truncating in case something went wrong in this script before
    create_table_sql = """
            CREATE TABLE IF NOT EXISTS temp_error_file (
                field_name text,
                error_message text,
                row_number integer,
                value_provided text,
                rule_label text,
                source_file integer,
                target_file integer,
                job_id integer,
                severity_id integer,
                filename text,
                error_type_id integer
            );
        """
    sess.execute(create_table_sql)
    sess.execute('TRUNCATE TABLE temp_error_file')
    sess.commit()

    # Loop through each unique certify history to get relevant details
    for certify_history in certify_history_list:
        logger.info('Moving error metadata from file: {}'.format(certify_history.warning_filename))
        warning_file_path = certify_history.warning_filename
        file_name = os.path.basename(warning_file_path)

        # If it's not local, we need to add the bucket to the stored path
        if not CONFIG_BROKER['local']:
            warning_file_path = 's3://' + CONFIG_BROKER['certified_bucket'] + '/' + warning_file_path

        with RetrieveFileFromUri(warning_file_path, 'r').get_file_object() as warning_file:
            warning_df = pd.read_csv(warning_file, dtype=str)

        # Only bother processing if there's actual data in the warning file
        if not warning_df.empty:
            # Cross-file and single file validations are slightly different so we have to treat them differently
            if 'cross_warning' in warning_file_path:
                field_map = {'Field names': 'field_name',
                             'Error message': 'error_message',
                             'Row number': 'row_number',
                             'Values provided': 'value_provided',
                             'Rule label': 'rule_label',
                             'Source File': 'source_file',
                             'Target File': 'target_file'}
                relevant_job = sess.query(Job).filter_by(submission_id=certify_history.submission_id,
                                                         job_type_id=JOB_TYPE_DICT['validation']).one()
                warning_df['filename'] = 'cross_file'
            else:
                field_map = {'Field name': 'field_name',
                             'Error message': 'error_message',
                             'Row number': 'row_number',
                             'Value provided': 'value_provided',
                             'Rule label': 'rule_label'}

                file_type_match = re.match('submission_{}_(.+)_warning_report.csv'.
                                           format(certify_history.submission_id), file_name)
                file_type = file_type_match.groups()[0]
                warning_df['source_file'] = file_type
                warning_df['target_file'] = None
                relevant_job = sess.query(Job).filter_by(submission_id=certify_history.submission_id,
                                                         job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                                                         file_type_id=FILE_TYPE_DICT[file_type]).one()
                warning_df['filename'] = relevant_job.filename

            warning_df['job_id'] = relevant_job.job_id
            warning_df['severity_id'] = RULE_SEVERITY_DICT['warning']
            warning_df.rename(columns=field_map, inplace=True)

            warning_df['source_file'] = warning_df.apply(lambda x: convert_file_type_to_int(x, 'source_file'), axis=1)
            warning_df['target_file'] = warning_df.apply(lambda x: convert_file_type_to_int(x, 'target_file'), axis=1)
            warning_df['error_type_id'] = warning_df.apply(lambda x: derive_error_type_id(x), axis=1)
            # Replace the word "None" anywhere in the dataframe with an actual None
            warning_df = warning_df.replace('None', np.nan)
            insert_dataframe(warning_df, 'temp_error_file', sess.connection())
            sess.commit()

            # Transfer contents of file to certified error metadata
            insert_sql = """
                INSERT INTO certified_error_metadata (
                    created_at,
                    updated_at,
                    job_id,
                    filename,
                    field_name,
                    error_type_id,
                    occurrences,
                    first_row,
                    rule_failed,
                    file_type_id,
                    target_file_type_id,
                    original_rule_label,
                    severity_id
                )
                SELECT
                    NOW(),
                    NOW(),
                    job_id,
                    filename,
                    field_name,
                    error_type_id,
                    COUNT(1),
                    MIN(row_number),
                    error_message,
                    source_file,
                    target_file,
                    rule_label,
                    severity_id
                FROM temp_error_file
                GROUP BY
                    job_id,
                    filename,
                    field_name,
                    error_type_id,
                    error_message,
                    source_file,
                    target_file,
                    rule_label,
                    severity_id
            """

            sess.execute(insert_sql)
            sess.execute('TRUNCATE TABLE temp_error_file')
            sess.commit()

    sess.execute('DROP TABLE temp_error_file')
    sess.commit()
    logger.info('Updated error metadata moved')


if __name__ == '__main__':
    db_sess = GlobalDB.db().session

    configure_logging()

    with create_app().app_context():
        move_certified_error_metadata(db_sess)
        move_updated_error_metadata(db_sess)
