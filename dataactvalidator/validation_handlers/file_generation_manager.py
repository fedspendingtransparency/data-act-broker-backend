import logging

from contextlib import contextmanager
from flask import Flask

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import mark_job_status
from dataactcore.models.jobModels import Job, FileRequest
from dataactcore.models.lookups import JOB_TYPE_DICT, JOB_STATUS_DICT
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

from dataactvalidator.validation_handlers.file_generation_handler import (
    generate_d_file, generate_e_file, generate_f_file, copy_parent_file_request_data)
from dataactvalidator.validation_handlers.validationError import ValidationError

logger = logging.getLogger(__name__)


class FileGenerationManager:
    def __init__(self, is_local=True):
        # Initialize instance variables
        self.is_local = is_local
        self.sess = GlobalDB.db().session

    def generate_from_job(self, job, agency_code):
        """Generate a file for a specified job
        Args:
            job:  Job to be validated
        Returns:
            JSONResponse object
        """
        # Make sure this is a file generation job
        if job.job_type.name != 'file_upload':
            raise ResponseException(
                'Job ID {} is not a file generation job (job type is {})'.format(job.job_id, job.job_type.name),
                StatusCode.CLIENT_ERROR, None, ValidationError.jobError)

        # Generate timestamped file names
        old_filename = job.original_filename
        job.original_filename = S3Handler.get_timestamped_filename(CONFIG_BROKER["".join([str(job.file_type.name),
                                                                                          "_file_name"])])
        if self.is_local:
            job.filename = "".join([CONFIG_BROKER['broker_files'], job.original_filename])
        else:
            job.filename = "".join([str(job.submission_id), "/", job.original_filename])

        with job_context(job, self.is_local) as sess:
            # Generate the file and upload to S3
            if job.file_type.letter_name in ['D1', 'D2']:
                # Update the validation Job if this has an attached Submission
                if job.submission_id:
                    self.update_validation_job_info(job)

                generate_d_file(sess, job, agency_code, self.is_local, old_filename)
            elif job.file_type.letter_name == 'E':
                generate_e_file(sess, job, self.is_local)
            else:
                generate_f_file(sess, job, self.is_local)

    def update_validation_job_info(self, job):
        """ Populates upload and validation job objects with start and end dates, filenames, and status.
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
def job_context(job, is_local=True):
    """Common context for files D1, D2, E, and F generation. Handles marking the job finished and/or failed"""
    # Flask context ensures we have access to global.g
    with Flask(__name__).app_context():
        sess = GlobalDB.db().session
        try:
            yield sess
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
