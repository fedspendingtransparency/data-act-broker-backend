import csv
import logging
import os
import smart_open

from datetime import datetime
from contextlib import contextmanager
from flask import Flask

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import mark_job_status
from dataactcore.models.domainModels import ExecutiveCompensation
from dataactcore.models.jobModels import Job, FileRequest
from dataactcore.models.lookups import (JOB_STATUS_DICT, JOB_STATUS_DICT_ID, JOB_TYPE_DICT, FILE_TYPE_DICT,
                                        FILE_TYPE_DICT_LETTER)
from dataactcore.models.stagingModels import AwardFinancialAssistance, AwardProcurement
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
                # mark job as failed
                job.error_message = str(e)
                sess.commit()
                mark_job_status(job_id, "failed")

                file_request = sess.query(FileRequest).filter_by(job_id=job_id).one_or_none()
                if file_request:
                    file_request.is_cached_file = False
                    sess.commit()
        finally:
            # update FileRequest and its children
            job = sess.query(Job).filter_by(job_id=job_id).one_or_none()
            file_request = sess.query(FileRequest).filter_by(job_id=job_id).one_or_none()
            if job and file_request:
                child_requests = sess.query(FileRequest).filter_by(parent_job_id=job_id).all()
                file_type = FILE_TYPE_DICT_LETTER[job.file_type_id]
                for child in child_requests:
                    copy_parent_file_request_data(sess, child.job, file_request.job, file_type, True)

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
    with job_context(job_id) as sess:
        # get a FileRequest object by job_id or create one
        current_date = datetime.now().date()
        file_request = sess.query(FileRequest).filter_by(job_id=job_id).one_or_none()
        if not file_request:
            file_request = FileRequest(request_date=current_date, job_id=job_id, start_date=start, end_date=end,
                                       agency_code=agency_code, file_type=file_type, is_cached_file=False)
            sess.add(file_request)

        # search for cached FileRequest to mark as parent FileRequest
        parent_req = sess.query(FileRequest).\
            filter_by(request_date=current_date, file_type=file_type, start_date=start, end_date=end,
                      agency_code=agency_code, is_cached_file=True).\
            one_or_none()
        file_request.parent_job_id = parent_req.job_id if parent_req else None
        sess.commit()

        if not parent_req:
            # generate D file
            file_utils = fileD1 if file_type == 'D1' else fileD2
            full_file_path = "".join([CONFIG_BROKER['d_file_storage_path'], file_name])
            page_idx = 0

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
                        query_data(sess, agency_code, start, end, page_start, (QUERY_SIZE * (page_idx + 1))).all()

                    if rows is None:
                        break

                    # write records to file
                    logger.debug('Writing rows {}-{} to {} CSV'.format(page_start, page_start + len(rows), file_type))
                    out_csv.writerows(rows)
                    if len(rows) < QUERY_SIZE:
                        break
                    page_idx += 1

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

            # mark this FileRequest as the cached version
            file_request.is_cached_file = True
            sess.commit()

            if not parent_req:
                # copy job data to all child FileRequests
                child_requests = sess.query(FileRequest).filter_by(parent_job_id=job_id).all()
                for child in child_requests:
                    copy_parent_file_request_data(sess, child.job, file_request.job, file_type, is_local)
        else:
            # copy parent data to this job
            file_request.job.is_cached = True
            sess.commit()
            if parent_req.job.job_status_id != JOB_STATUS_DICT['running']:
                # only copy file data if job has finished running
                copy_parent_file_request_data(sess, file_request.job, parent_req.job, file_type, is_local)

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


def copy_parent_file_request_data(sess, child_job, parent_job, file_type, is_local):
    """ Copy parent FileRequest job data to the child FileRequest job data

        Args:
            sess - current DB session
            child_job - Job ID for the child FileRequest object
            parent_job - Job ID for the parent FileRequest object
            file_type - File type as either "D1" or "D2"
            is_local - True if in local development, False otherwise
    """
    logger.debug('Copying job {} data to job {}'.format(parent_job.job_id, child_job.job_id))
    filename = '{}/{}'.format(child_job.filename.rsplit('/', 1)[0], parent_job.original_filename)

    child_job.filename = filename
    child_job.original_filename = parent_job.original_filename
    mark_job_status(child_job.job_id, JOB_STATUS_DICT_ID[parent_job.job_status_id])
    child_job.number_of_rows = parent_job.number_of_rows
    child_job.number_of_rows_valid = parent_job.number_of_rows_valid
    child_job.number_of_errors = parent_job.number_of_errors
    child_job.number_of_warnings = parent_job.number_of_warnings
    child_job.error_message = parent_job.error_message
    # change the validation job file data for in-submission D file generations
    if child_job.submission_id is not None:
        val_job = sess.query(Job).filter(Job.submission_id == child_job.submission_id,
                                         Job.file_type_id == FILE_TYPE_DICT[file_type],
                                         Job.job_type_id == JOB_TYPE_DICT['csv_record_validation']).one()
        val_job.filename = filename
        val_job.original_filename = parent_job.original_filename
    sess.commit()

    if not is_local:
        with smart_open.smart_open(S3Handler.create_file_path(parent_job.filename), 'r') as reader:
            with smart_open.smart_open(S3Handler.create_file_path(child_job.filename), 'w') as writer:
                while True:
                    chunk = reader.read(CHUNK_SIZE)
                    if chunk:
                        writer.write(chunk)
                    else:
                        break
