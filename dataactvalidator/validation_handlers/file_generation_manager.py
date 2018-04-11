import logging

from contextlib import contextmanager
from flask import Flask

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import mark_job_status
from dataactcore.models.jobModels import Job, FileRequest
from dataactcore.models.lookups import (JOB_TYPE_DICT, JOB_STATUS_DICT, JOB_STATUS_DICT_ID, FILE_TYPE_DICT_LETTER,
                                        FILE_TYPE_DICT_LETTER_ID)
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

from dataactvalidator.validation_handlers.file_generation_handler import (
    generate_d_file, generate_e_file, generate_f_file, copy_parent_file_request_data)
from dataactvalidator.validation_handlers.validationError import ValidationError

logger = logging.getLogger(__name__)
STATUS_MAP = {"waiting": "waiting", "ready": "invalid", "running": "waiting", "finished": "finished",
              "invalid": "failed", "failed": "failed"}
VALIDATION_STATUS_MAP = {"waiting": "waiting", "ready": "waiting", "running": "waiting", "finished": "finished",
                         "failed": "failed", "invalid": "failed"}


class FileGenerationManager:
    def __init__(self, is_local=True):
        # Initialize instance variables
        self.is_local = is_local
        self.sess = GlobalDB.db().session

    def generate_from_job(self, job_id, agency_code):
        """Generate a file for a specified job
        Args:
            job         -- upload Job
            agency_code -- FREC or CGAC code to generate data from
        """
        with job_context(job_id, self.is_local) as context:
            sess, job = context

            # Ensure this is a file generation job
            if job.job_type.name != 'file_upload':
                raise ResponseException(
                    'Job ID {} is not a file generation job (job type is {})'.format(job.job_id, job.job_type.name),
                    StatusCode.CLIENT_ERROR, None, ValidationError.jobError)

            # Ensure there is an available agency_code
            if not agency_code:
                if job.submission_id:
                    agency_code = job.submission.frec_code if job.submission.frec_code else job.submission.cgac_code
                else:
                    raise ResponseException(
                        'An agency_code must be provided to generate a file'.format(job.job_id, job.job_type.name),
                        StatusCode.CLIENT_ERROR, None, ValidationError.jobError)

            # Generate timestamped file names
            old_filename = job.original_filename
            job.original_filename = S3Handler.get_timestamped_filename(CONFIG_BROKER["".join([str(job.file_type.name),
                                                                                              "_file_name"])])
            if self.is_local:
                job.filename = "".join([CONFIG_BROKER['broker_files'], job.original_filename])
            else:
                job.filename = "".join([str(job.submission_id), "/", job.original_filename])

            # Generate the file and upload to S3
            if job.file_type.letter_name in ['D1', 'D2']:
                # Update the validation Job if necessary
                if job.submission_id:
                    self.update_validation_job_info(job)

                generate_d_file(sess, job, agency_code, self.is_local, old_filename)
            elif job.file_type.letter_name == 'E':
                generate_e_file(sess, job, self.is_local)
            else:
                generate_f_file(sess, job, self.is_local)

    def update_validation_job_info(self, job):
        """ Populates validation job objects with start and end dates, filenames, and status.
            Assumes the upload Job's start and end dates have been validated.

        Args:
            job - upload Job
        """
        # Retrieve and update the validation Job
        val_job = self.sess.query(Job).filter(Job.submission_id == job.submission_id,
                                              Job.file_type_id == job.file_type_id,
                                              Job.job_type_id == JOB_TYPE_DICT['csv_record_validation']).one()
        val_job.start_date = job.start_date
        val_job.end_date = job.end_date
        val_job.filename = job.filename
        val_job.original_filename = job.original_filename
        val_job.job_status_id = JOB_STATUS_DICT["waiting"]

        # Clear out error messages to prevent stale messages
        job.error_message = None
        val_job.error_message = None

        self.sess.commit()


@contextmanager
def job_context(job_id, is_local=True):
    """Common context for files D1, D2, E, and F generation. Handles marking the job finished and/or failed"""
    # Flask context ensures we have access to global.g
    with Flask(__name__).app_context():
        sess = GlobalDB.db().session
        job = sess.query(Job).filter(Job.job_id == job_id).one_or_none()
        try:
            yield sess, job
            if not job.from_cached:
                # only mark completed jobs as done
                logger.info({'message': 'Marking job {} as finished'.format(job.job_id), 'message_type': 'BrokerInfo',
                             'job_id': job.job_id})
                mark_job_status(job.job_id, "finished")
        except Exception as e:
            # logger.exception() automatically adds traceback info
            logger.exception({'message': 'Marking job {} as failed'.format(job.job_id), 'job_id': job.job_id,
                              'message_type': 'BrokerException', 'exception': str(e)})

            # mark job as failed
            job.error_message = str(e)
            mark_job_status(job.job_id, "failed")

            # ensure FileRequest from failed job is not cached
            file_request = sess.query(FileRequest).filter_by(job_id=job.job_id).one_or_none()
            if file_request and file_request.is_cached_file:
                file_request.is_cached_file = False

            sess.commit()

        finally:
            file_request = sess.query(FileRequest).filter_by(job_id=job.job_id).one_or_none()
            if file_request and file_request.is_cached_file:
                # copy job data to all child FileRequests
                child_requests = sess.query(FileRequest).filter_by(parent_job_id=job.job_id).all()
                if len(child_requests) > 0:
                    logger.info({'message': 'Copying file data from job {} to its children'.format(job.job_id),
                                 'message_type': 'BrokerInfo', 'job_id': job.job_id})
                    for child in child_requests:
                        copy_parent_file_request_data(sess, child.job, job, is_local)
            GlobalDB.close()


def check_file_generation(job_id):
    """Check the status of a file generation
    Args:
        job_id -- upload Job ID
    Return:
        Dict with keys: job_id, status, file_type, message, url, start, end
    """
    sess = GlobalDB.db().session

    # We want to use one_or_none() here so we can see if the job is None so we can mark the status as invalid to
    # indicate that a status request is invoked for a job that isn't created yet
    upload_job = sess.query(Job).filter_by(job_id=job_id).one_or_none()
    response_dict = {'job_id': job_id, 'status': '', 'file_type': '', 'message': '', 'url': '#'}

    if upload_job is None:
        response_dict['start'] = ''
        response_dict['end'] = ''
        response_dict['status'] = 'invalid'
        response_dict['message'] = 'No generation job found with the specified ID'
        return response_dict

    response_dict['status'] = map_generate_status(sess, upload_job)
    response_dict['file_type'] = FILE_TYPE_DICT_LETTER[upload_job.file_type_id]
    response_dict['message'] = upload_job.error_message or ''

    # Generate the URL (or path) to the file
    if CONFIG_BROKER['use_aws'] and response_dict['status'] is 'finished' and upload_job.filename:
        path, file_name = upload_job.filename.split('/')
        response_dict['url'] = S3Handler().get_signed_url(path=path, file_name=file_name, bucket_route=None,
                                                          method='GET')
    elif response_dict['status'] is 'finished' and upload_job.filename:
        response_dict['url'] = upload_job.filename

    # Only D file generations have start and end dates
    if response_dict['file_type'] in ['D1', 'D2']:
        response_dict['start'] = upload_job.start_date.strftime("%m/%d/%Y") if upload_job.start_date is not None else ""
        response_dict['end'] = upload_job.end_date.strftime("%m/%d/%Y") if upload_job.end_date is not None else ""

    return response_dict


def map_generate_status(sess, upload_job):
    """ Maps job status to file generation statuses expected by frontend """
    print(upload_job.__dict__)
    if FILE_TYPE_DICT_LETTER[upload_job.file_type_id] in ['D1', 'D2'] and upload_job.submission_id:
        validation_job = sess.query(Job).filter(Job.submission_id == upload_job.submission_id,
                                                Job.file_type_id == upload_job.file_type_id,
                                                Job.job_type_id == JOB_TYPE_DICT['csv_record_validation']).one()
        validation_status = validation_job.job_status.name
        if validation_job.number_of_errors > 0:
            errors_present = True
        else:
            errors_present = False
    else:
        validation_job = None
        errors_present = False

    response_status = STATUS_MAP[upload_job.job_status.name]
    if response_status == "failed" and upload_job.error_message in ['', None]:
        # Provide an error message if none present
        upload_job.error_message = "Upload job failed without error message"

    if validation_job is None:
        # No validation job, so don't need to check it
        sess.commit()
        return response_status

    if response_status == "finished":
        # Check status of validation job if present
        response_status = VALIDATION_STATUS_MAP[validation_status]
        if response_status == "finished" and errors_present:
            # If validation completed with errors, mark as failed
            response_status = "failed"
            upload_job.error_message = "Validation completed but row-level errors were found"

    if response_status == "failed":
        if upload_job.error_message in ['', None] and validation_job.error_message in ['', None]:
            if validation_status == "invalid":
                upload_job.error_message = "Generated file had file-level errors"
            else:
                upload_job.error_message = "Validation job had an internal error"

        elif upload_job.error_message in ['', None]:
            upload_job.error_message = validation_job.error_message

    sess.commit()
    return response_status
