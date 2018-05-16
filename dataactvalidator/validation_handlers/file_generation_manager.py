import logging

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import mark_job_status
from dataactcore.models.jobModels import Job
from dataactcore.models.lookups import JOB_TYPE_DICT, JOB_STATUS_DICT
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

from dataactvalidator.validation_handlers.file_generation_handler import (job_context, generate_d_file, generate_e_file,
                                                                          generate_f_file)
from dataactvalidator.validation_handlers.validationError import ValidationError

logger = logging.getLogger(__name__)


class FileGenerationManager:
    """ Responsible for managing the generation of all files

        Attributes:
            is_local: A boolean flag indicating whether the application is being run locally or not
            sess: Current database session
    """

    def __init__(self, is_local=True):
        """ Initialize the FileGeneration Manager

            Args:
                is_local: True if this is a local installation that will not use AWS
        """
        self.is_local = is_local
        self.sess = GlobalDB.db().session

    def generate_from_job(self, job_id, agency_code):
        """ Generates a file for a specified job
            Args:
                job: upload Job
                agency_code: FREC or CGAC code to generate data from
        """
        mark_job_status(job_id, 'running')

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
                job: upload Job
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
