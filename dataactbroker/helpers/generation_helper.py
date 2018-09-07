import logging

from datetime import datetime
from flask import g
from sqlalchemy import or_

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.aws.sqsHandler import sqs_queue
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import mark_job_status
from dataactcore.models import lookups
from dataactcore.models.jobModels import Job, Submission

from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.stringCleaner import StringCleaner

logger = logging.getLogger(__name__)

STATUS_MAP = {"waiting": "waiting", "ready": "invalid", "running": "waiting", "finished": "finished",
              "invalid": "failed", "failed": "failed"}
VALIDATION_STATUS_MAP = {"waiting": "waiting", "ready": "waiting", "running": "waiting", "finished": "finished",
                         "failed": "failed", "invalid": "failed"}


def start_d_generation(job, start_date, end_date, agency_type, agency_code=None):
    """ Validates the start and end dates of the generation, updates the submission's publish status and progress (if
        its not detached generation), and sends the job information to SQS.

        Args:
            job: File generation job to start
            start_date: String to parse as the start date of the generation
            end_date: String to parse as the end date of the generation
            agency_type: Type of Agency to generate files by: "awarding" or "funding"
            agency_code: Agency code for detached D file generations

        Returns:
            SQS send_message response
    """
    if not (StringCleaner.is_date(start_date) and StringCleaner.is_date(end_date)):
        raise ResponseException("Start or end date cannot be parsed into a date of format MM/DD/YYYY",
                                StatusCode.CLIENT_ERROR)

    # Update the Job's start and end dates
    sess = GlobalDB.db().session
    job.start_date = start_date
    job.end_date = end_date
    sess.commit()

    # Update submission
    if job.submission_id:
        submission = sess.query(Submission).filter(Submission.submission_id == job.submission_id).one()

        # Change the publish status back to updated if certified
        if submission.publish_status_id == lookups.PUBLISH_STATUS_DICT['published']:
            submission.publishable = False
            submission.publish_status_id = lookups.PUBLISH_STATUS_DICT['updated']
            submission.updated_at = datetime.utcnow()
            sess.commit()

        # Set cross-file validation status to waiting if it's not already
        cross_file_job = sess.query(Job).filter(Job.submission_id == job.submission_id,
                                                Job.job_type_id == lookups.JOB_TYPE_DICT['validation'],
                                                Job.job_status_id != lookups.JOB_STATUS_DICT['waiting']).one_or_none()

        # No need to update it for each type of D file generation job, just do it once
        if cross_file_job:
            cross_file_job.job_status_id = lookups.JOB_STATUS_DICT['waiting']
            sess.commit()

    mark_job_status(job.job_id, "waiting")

    logger.info({
        'message': 'Sending {} file generation job {} to SQS'.format(job.file_type.letter_name, job.job_id),
        'message_type': 'BrokerInfo',
        'submission_id': job.submission_id,
        'job_id': job.job_id,
        'file_type': job.file_type.letter_name
    })

    # Set SQS message attributes
    message_attr = {'agency_type': {'DataType': 'String', 'StringValue': agency_type}}
    if agency_code:
        message_attr['agency_code'] = {'DataType': 'String', 'StringValue': agency_code}

    # Add job_id to the SQS job queue
    queue = sqs_queue()
    return queue.send_message(MessageBody=str(job.job_id), MessageAttributes=message_attr)


def start_e_f_generation(job):
    """ Passes the Job ID for an E or F generation Job to SQS

        Args:
            job: File generation job to start
    """
    mark_job_status(job.job_id, "waiting")

    file_type = job.file_type.letter_name
    logger.info({
        'message': 'Sending {} file generation job {} to Validator in SQS'.format(file_type, job.job_id),
        'message_type': 'BrokerInfo',
        'submission_id': job.submission_id,
        'job_id': job.job_id,
        'file_type': file_type
    })

    # Add job_id to the SQS job queue
    queue = sqs_queue()
    return queue.send_message(MessageBody=str(job.job_id), MessageAttributes={})


def check_file_generation(job_id):
    """ Check the status of a file generation

        Args:
            job_id: upload Job ID
        Return:
            Dict with keys: job_id, status, file_type, message, url, start, end
    """
    sess = GlobalDB.db().session

    # We want to use one_or_none() here so we can see if the job is None so we can mark the status as invalid to
    # indicate that a status request is invoked for a job that isn't created yet
    upload_job = sess.query(Job).filter_by(job_id=job_id).one_or_none()
    response_dict = {'job_id': job_id, 'status': '', 'file_type': '', 'message': '', 'url': '#', 'size': None}

    if upload_job is None:
        response_dict['start'] = ''
        response_dict['end'] = ''
        response_dict['status'] = 'invalid'
        response_dict['message'] = 'No generation job found with the specified ID'
        return response_dict

    response_dict['file_type'] = lookups.FILE_TYPE_DICT_LETTER[upload_job.file_type_id]
    response_dict['size'] = upload_job.file_size
    response_dict['status'] = map_generate_status(sess, upload_job)
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
    """ Maps job status to file generation statuses expected by frontend. Updates the error message of the job, if
        there is one.

        Args:
            upload_job: the upload job for this file
            validation_job: the validation job for this file if applicable

        Returns:
            The status of the submission based on upload job status and validation job status (where applicable)
    """
    if lookups.FILE_TYPE_DICT_LETTER[upload_job.file_type_id] in ['D1', 'D2'] and upload_job.submission_id:
        validation_job = sess.query(Job).filter(Job.submission_id == upload_job.submission_id,
                                                Job.file_type_id == upload_job.file_type_id,
                                                Job.job_type_id == lookups.JOB_TYPE_DICT['csv_record_validation']).one()
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


def add_generation_job_info(file_type_name, job=None, start_date=None, end_date=None):
    """ Add details to jobs for generating files

        Args:
            file_type_name: the name of the file type being generated
            job: the generation job, None if it is a detached generation
            start_date: The start date for the generation job, only used for detached files
            end_date: The end date for the generation job, only used for detached files

        Returns:
            the file generation job
    """
    sess = GlobalDB.db().session

    # Create a new job for a detached generation
    if job is None:
        job = Job(job_type_id=lookups.JOB_TYPE_DICT['file_upload'], user_id=g.user.user_id,
                  file_type_id=lookups.FILE_TYPE_DICT[file_type_name], start_date=start_date, end_date=end_date)
        sess.add(job)

    # Update the job details
    job.message = None
    job.job_status_id = lookups.JOB_STATUS_DICT["ready"]
    sess.commit()
    sess.refresh(job)

    return job


def check_generation_prereqs(submission_id, file_type):
    """ Make sure the prerequisite jobs for this file type are complete without errors.

        Args:
            submission_id: the submission id for which we're checking file generation prerequisites
            file_type: the type of file being generated

        Returns:
            A boolean indicating if the job has no incomplete prerequisites (True if the job is clear to start)
    """

    sess = GlobalDB.db().session
    prereq_query = sess.query(Job).filter(Job.submission_id == submission_id,
                                          or_(Job.job_status_id != lookups.JOB_STATUS_DICT['finished'],
                                              Job.number_of_errors > 0))

    # Check cross-file validation if generating E or F
    if file_type in ['E', 'F']:
        unfinished_prereqs = prereq_query.filter(Job.job_type_id == lookups.JOB_TYPE_DICT['validation']).count()
    # Check A, B, C files if generating a D file
    elif file_type in ['D1', 'D2']:
        unfinished_prereqs = prereq_query.filter(Job.file_type_id.in_(
            [lookups.FILE_TYPE_DICT['appropriations'], lookups.FILE_TYPE_DICT['program_activity'],
             lookups.FILE_TYPE_DICT['award_financial']])).count()
    else:
        raise ResponseException('Invalid type for file generation', StatusCode.CLIENT_ERROR)

    return unfinished_prereqs == 0
