import csv
import logging
import os
import smart_open

from contextlib import contextmanager
from flask import Flask

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import mark_job_status
from dataactcore.models.jobModels import Job
from dataactcore.models.stagingModels import AwardFinancialAssistance, AwardProcurement
from dataactcore.models.domainModels import ExecutiveCompensation
from dataactcore.utils import fileD1, fileD2, fileE, fileF
from dataactvalidator.filestreaming.csv_selection import write_csv


logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024
QUERY_SIZE = 10000


@contextmanager
def job_context(job_id):
    """Common context for files D1, D2, E, and F generation. Handles marking the job finished and/or failed"""
    # Flask context ensures we have access to global.g
    with Flask(__name__).app_context():
        sess = GlobalDB.db().session
        try:
            yield sess
            logger.debug('Marking job as finished')
            mark_job_status(job_id, "finished")
        except Exception as e:
            # logger.exception() automatically adds traceback info
            logger.exception('Job %s failed', job_id)
            job = sess.query(Job).filter_by(job_id=job_id).one_or_none()
            if job:
                job.error_message = str(e)
                sess.commit()
                mark_job_status(job_id, "failed")
        finally:
            GlobalDB.close()


def generate_d_file(file_type, agency_code, start, end, job_id, file_name, upload_name, is_local):
    """ Write file D1 or D2 to an appropriate CSV.

        Args:
            file_type - File type as either "D1" or "D2"
            agency_code - FREC or CGAC code for generation
            start - Beginning of period for D file
            end - End of period for D file
            job_id - Job ID for upload job
            file_name - Version of filename without user ID
            upload_name - Filename to use on S3
            is_local - True if in local development, False otherwise
    """
    logger.debug('Starting file {} generation'.format(file_type))

    with job_context(job_id) as session:
        file_utils = fileD1 if file_type == 'D1' else fileD2
        full_file_path = "".join([CONFIG_BROKER['d_file_storage_path'], file_name])
        page_idx = 0

        try:
            # create file locally
            with open(full_file_path, 'w', newline='') as csv_file:
                out_csv = csv.writer(csv_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')

                # write headers to file
                headers = [key for key in file_utils.mapping]
                out_csv.writerow(headers)
                while True:
                    # query QUERY_SIZE number of rows
                    page_start = QUERY_SIZE * page_idx
                    rows = file_utils.\
                        query_data(session, agency_code, start, end, page_start, (QUERY_SIZE * (page_idx + 1))).all()

                    if rows is None:
                        break

                    # write records to file
                    logger.debug('Writing rows {}-{} to file {} CSV'.format(page_start, page_start + len(rows),
                                                                            file_type))
                    out_csv.writerows(rows)

                    if len(rows) < QUERY_SIZE:
                        break

                    page_idx += 1
        finally:
            # close file
            csv_file.close()

        if not is_local:
            # stream file to S3 when not local
            with open(full_file_path, 'rb') as csv_file:
                with smart_open.smart_open(S3Handler.create_file_path(upload_name), 'w') as writer:
                    while True:
                        chunk = csv_file.read(CHUNK_SIZE)
                        if chunk:
                            writer.write(chunk)
                        else:
                            break
            csv_file.close()
            os.remove(full_file_path)

        logger.debug('Finished writing to file: {}'.format(file_name))
    logger.debug('Finished file {} generation'.format(file_type))


def generate_f_file(submission_id, job_id, timestamped_name, upload_file_name, is_local):
    """ Write rows from fileF.generate_f_rows to an appropriate CSV.

        Args:
            submission_id - Submission ID for generation
            job_id - Job ID for upload job
            timestamped_name - Version of filename without user ID
            upload_file_name - Filename to use on S3
            is_local - True if in local development, False otherwise
    """
    logger.debug('Starting file F generation')

    with job_context(job_id):
        logger.debug('Calling generate_f_rows')
        rows_of_dicts = fileF.generate_f_rows(submission_id)
        header = [key for key in fileF.mappings]    # keep order
        body = []
        for row in rows_of_dicts:
            body.append([row[key] for key in header])

        logger.debug('Writing file F CSV')
        write_csv(timestamped_name, upload_file_name, is_local, header, body)

    logger.debug('Finished file F generation')


def generate_e_file(submission_id, job_id, timestamped_name, upload_file_name, is_local):
    """ Write file E to an appropriate CSV.

        Args:
            submission_id - Submission ID for generation
            job_id - Job ID for upload job
            timestamped_name - Version of filename without user ID
            upload_file_name - Filename to use on S3
            is_local - True if in local development, False otherwise
    """
    logger.debug('Starting file E generation')

    with job_context(job_id) as session:
        d1 = session.query(AwardProcurement.awardee_or_recipient_uniqu).\
            filter(AwardProcurement.submission_id == submission_id).\
            distinct()
        d2 = session.query(AwardFinancialAssistance.awardee_or_recipient_uniqu).\
            filter(AwardFinancialAssistance.submission_id == submission_id).\
            distinct()
        duns_set = {r.awardee_or_recipient_uniqu for r in d1.union(d2)}
        duns_list = list(duns_set)    # get an order

        rows = []
        for i in range(0, len(duns_list), 100):
            rows.extend(fileE.retrieve_rows(duns_list[i:i + 100]))

        # Add rows to database here.
        # TODO: This is a temporary solution until loading from SAM's SFTP has been resolved
        for row in rows:
            session.merge(ExecutiveCompensation(**fileE.row_to_dict(row)))
        session.commit()

        logger.debug('Writing file E CSV')
        write_csv(timestamped_name, upload_file_name, is_local, fileE.Row._fields, rows)

    logger.debug('Finished file E generation')
