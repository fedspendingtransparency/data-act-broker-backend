import boto3
import logging
import smart_open

from datetime import datetime
from contextlib import contextmanager
from flask import Flask

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import mark_job_status
from dataactcore.models.domainModels import ExecutiveCompensation
from dataactcore.models.jobModels import Job, FileRequest, FPDSUpdate
from dataactcore.models.lookups import (JOB_STATUS_DICT, JOB_STATUS_DICT_ID, JOB_TYPE_DICT, FILE_TYPE_DICT_LETTER_ID,
                                        FILE_TYPE_DICT_LETTER, FILE_TYPE_DICT_LETTER_NAME)
from dataactcore.models.stagingModels import AwardFinancialAssistance, AwardProcurement
from dataactcore.utils import fileD1, fileD2, fileE, fileF
from dataactvalidator.filestreaming.csv_selection import write_csv, write_query_to_file, stream_file_to_s3

logger = logging.getLogger(__name__)


@contextmanager
def job_context(job_id, is_local=True):
    """Common context for files D1, D2, E, and F generation. Handles marking the job finished and/or failed"""
    # Flask context ensures we have access to global.g
    with Flask(__name__).app_context():
        sess = GlobalDB.db().session
        try:
            yield sess
            logger.info({
                'message': 'Marking job {} as finished'.format(job_id),
                'message_type': 'BrokerInfo',
                'job_id': job_id
            })
            mark_job_status(job_id, "finished")
        except Exception as e:
            # logger.exception() automatically adds traceback info
            logger.exception({
                'message': 'Marking job {} as failed'.format(job_id),
                'message_type': 'BrokerException',
                'job_id': job_id,
                'exception': str(e)
            })
            job = sess.query(Job).filter_by(job_id=job_id).one_or_none()
            if job:
                # mark job as failed
                job.error_message = str(e)
                sess.commit()
                mark_job_status(job_id, "failed")

                # ensure FileRequest from failed job is not cached
                file_request = sess.query(FileRequest).filter_by(job_id=job_id).one_or_none()
                if file_request and file_request.is_cached_file:
                    file_request.is_cached_file = False
                    sess.commit()
        finally:
            file_request = sess.query(FileRequest).filter_by(job_id=job_id).one_or_none()
            if file_request and file_request.is_cached_file:
                # copy job data to all child FileRequests
                child_requests = sess.query(FileRequest).filter_by(parent_job_id=job_id).all()
                file_type = FILE_TYPE_DICT_LETTER[file_request.job.file_type_id]
                for child in child_requests:
                    copy_parent_file_request_data(sess, child.job, file_request.job, file_type, is_local)

            GlobalDB.close()


def generate_d_file(file_type, agency_code, start, end, job_id, upload_name, is_local, submission_id=None):
    """Write file D1 or D2 to an appropriate CSV.

        Args:
            file_type - File type as either "D1" or "D2"
            agency_code - FREC or CGAC code for generation
            start - Beginning of period for D file
            end - End of period for D file
            job_id - Job ID for upload job
            upload_name - File key to use on S3
            is_local - True if in local development, False otherwise
    """
    log_data = {
        'message_type': 'BrokerInfo',
        'job_id': job_id,
        'file_type': FILE_TYPE_DICT_LETTER_NAME[file_type],
        'agency_code': agency_code,
        'start_date': start,
        'end_date': end
    }
    if submission_id:
        log_data['submission_id'] = submission_id

    with job_context(job_id, is_local) as sess:
        current_date = datetime.now().date()

        # check if FileRequest already exists with this job_id, if not, create one
        file_request = sess.query(FileRequest).filter(FileRequest.job_id == job_id).one_or_none()
        if not file_request:
            file_request = FileRequest(request_date=current_date, job_id=job_id, start_date=start, end_date=end,
                                       agency_code=agency_code, file_type=file_type, is_cached_file=False)
            sess.add(file_request)

        # search for potential parent FileRequests
        parent_file_request = None
        if not file_request.is_cached_file:
            parent_request_query = sess.query(FileRequest).\
                filter(FileRequest.file_type == file_type, FileRequest.start_date == start, FileRequest.end_date == end,
                       FileRequest.agency_code == agency_code, FileRequest.is_cached_file.is_(True))

            # filter D1 FileRequests by the date of the last FPDS pull
            if file_type == 'D1':
                last_update = sess.query(FPDSUpdate).one_or_none()
                fpds_date = last_update.update_date if last_update else current_date
                parent_request_query = parent_request_query.filter(FileRequest.request_date >= fpds_date)

            # mark FileRequest with parent job_id
            parent_file_request = parent_request_query.one_or_none()
            file_request.parent_job_id = parent_file_request.job_id if parent_file_request else None
        sess.commit()

        if file_request.is_cached_file:
            # this is the cached file, no need to do anything
            log_data['message'] = '{} file has already been generated by this job'.format(file_type)
            logger.info(log_data)
        elif parent_file_request:
            # copy parent data to this job if parent is not still running
            if parent_file_request.job.job_status_id != JOB_STATUS_DICT['running']:
                copy_parent_file_request_data(sess, file_request.job, parent_file_request.job, file_type, is_local)
        else:
            # no cached file
            file_name = upload_name.split('/')[-1]
            log_data['message'] = 'Starting file {} generation'.format(file_type)
            log_data['file_name'] = file_name
            logger.info(log_data)

            # mark this FileRequest as the cached version
            file_request.is_cached_file = True
            sess.commit()

            file_utils = fileD1 if file_type == 'D1' else fileD2
            local_filename = "".join([CONFIG_BROKER['d_file_storage_path'], file_name])
            headers = [key for key in file_utils.mapping]

            # actually generate the file
            query_utils = {"file_utils": file_utils, "agency_code": agency_code, "start": start, "end": end,
                           "sess": sess}
            write_query_to_file(local_filename, upload_name, headers, file_type, is_local, d_file_query, query_utils)

            log_data['message'] = 'Finished writing to file: {}'.format(file_name)
            logger.info(log_data)
    log_data['message'] = 'Finished file {} generation'.format(file_type)
    logger.info(log_data)


def generate_f_file(submission_id, job_id, timestamped_name, upload_file_name, is_local):
    """Write rows from fileF.generate_f_rows to an appropriate CSV.

        Args:
            submission_id - Submission ID for generation
            job_id - Job ID for upload job
            timestamped_name - Version of filename without user ID
            upload_file_name - Filename to use on S3
            is_local - True if in local development, False otherwise
    """
    log_data = {
        'message': 'Starting file F generation',
        'message_type': 'BrokerInfo',
        'submission_id': submission_id,
        'job_id': job_id,
        'file_type': 'sub_award'
    }
    logger.info(log_data)

    with job_context(job_id):
        rows_of_dicts = fileF.generate_f_rows(submission_id)
        header = [key for key in fileF.mappings]    # keep order
        body = []
        for row in rows_of_dicts:
            body.append([row[key] for key in header])

        log_data['message'] = 'Writing file F CSV'
        logger.info(log_data)
        write_csv(timestamped_name, upload_file_name, is_local, header, body)

    log_data['message'] = 'Finished file F generation'
    logger.info(log_data)


def generate_e_file(submission_id, job_id, timestamped_name, upload_file_name, is_local):
    """Write file E to an appropriate CSV.

        Args:
            submission_id - Submission ID for generation
            job_id - Job ID for upload job
            timestamped_name - Version of filename without user ID
            upload_file_name - Filename to use on S3
            is_local - True if in local development, False otherwise
    """
    log_data = {
        'message': 'Starting file E generation',
        'message_type': 'BrokerInfo',
        'submission_id': submission_id,
        'job_id': job_id,
        'file_type': 'executive_compensation'
    }
    logger.info(log_data)

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

        log_data['message'] = 'Writing file E CSV'
        logger.info(log_data)
        write_csv(timestamped_name, upload_file_name, is_local, fileE.Row._fields, rows)

    log_data['message'] = 'Finished file E generation'
    logger.info(log_data)


def d_file_query(query_utils, page_start, page_end):
    """Retrieve D1 or D2 data.

        Args:
            query_utils - object containing:
                file_utils - fileD1 or fileD2 utils
                sess - DB session
                agency_code - FREC or CGAC code for generation
                start - Beginning of period for D file
                end - End of period for D file
            page_start - Beginning of pagination
            page_stop - End of pagination

        Return:
            paginated D1 or D2 query results
    """
    rows = query_utils["file_utils"].query_data(query_utils["sess"], query_utils["agency_code"], query_utils["start"],
                                                query_utils["end"], page_start, page_end)
    return rows.all()


def copy_parent_file_request_data(sess, child_job, parent_job, file_type, is_local):
    """Parent FileRequest job data to the child FileRequest job data.

        Args:
            sess - current DB session
            child_job - Job ID for the child FileRequest object
            parent_job - Job ID for the parent FileRequest object
            file_type - File type as either "D1" or "D2"
            is_local - True if in local development, False otherwise
    """
    log_data = {
        'message': 'Copying data from parent job with job_id:{}'.format(parent_job.job_id),
        'message_type': 'BrokerInfo',
        'job_id': child_job.job_id,
        'file_type': FILE_TYPE_DICT_LETTER_NAME[file_type]
    }

    # keep path but update file name
    filename = '{}/{}'.format(child_job.filename.rsplit('/', 1)[0], parent_job.original_filename)

    # copy parent job's data
    child_job.is_cached = True
    child_job.filename = filename
    child_job.original_filename = parent_job.original_filename
    child_job.number_of_rows = parent_job.number_of_rows
    child_job.number_of_rows_valid = parent_job.number_of_rows_valid
    child_job.number_of_errors = parent_job.number_of_errors
    child_job.number_of_warnings = parent_job.number_of_warnings
    child_job.error_message = parent_job.error_message

    # change the validation job's file data when within a submission
    if child_job.submission_id is not None:
        val_job = sess.query(Job).filter(Job.submission_id == child_job.submission_id,
                                         Job.file_type_id == FILE_TYPE_DICT_LETTER_ID[file_type],
                                         Job.job_type_id == JOB_TYPE_DICT['csv_record_validation']).one()
        val_job.filename = filename
        val_job.original_filename = parent_job.original_filename
    sess.commit()

    if not is_local and parent_job.filename != child_job.filename:
        # check to see if the same file exists in the child bucket
        s3 = boto3.client('s3', region_name=CONFIG_BROKER["aws_region"])
        response = s3.list_objects_v2(Bucket=CONFIG_BROKER['aws_bucket'], Prefix=child_job.filename)
        for obj in response.get('Contents', []):
            if obj['Key'] == child_job.filename:
                # the file already exists in this location
                log_data['message'] = 'Cached {} file CSV already exists in this location'.format(file_type)
                logger.info(log_data)
                return

        # copy the parent file into the child's S3 location
        log_data['message'] = 'Copying the cached {} file from job {}'.format(file_type, parent_job.job_id)
        logger.info(log_data)
        with smart_open.smart_open(S3Handler.create_file_path(parent_job.filename), 'r') as reader:
            stream_file_to_s3(child_job.filename, reader)

    # mark job status last so the validation job doesn't start until everything is done
    mark_job_status(child_job.job_id, JOB_STATUS_DICT_ID[parent_job.job_status_id])
