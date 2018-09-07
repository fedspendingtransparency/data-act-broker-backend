import boto3
import logging
import smart_open

from datetime import datetime
from flask import g
from sqlalchemy import or_

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.aws.sqsHandler import sqs_queue
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import mark_job_status
from dataactcore.models import lookups
from dataactcore.models.jobModels import FileRequest, FPDSUpdate, Job, Submission
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.stringCleaner import StringCleaner

from dataactvalidator.filestreaming.csv_selection import stream_file_to_s3

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
        agency_code = submission.frec_code if submission.frec_code else submission.cgac_code

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

    log_data = {'message': 'Sending {} file generation job {} to SQS'.format(job.file_type.letter_name, job.job_id),
                'message_type': 'BrokerInfo', 'submission_id': job.submission_id, 'job_id': job.job_id,
                'file_type': job.file_type.letter_name}
    logger.info(log_data)

    file_request = retrieve_cached_file_request(job, agency_type, agency_code)
    if file_request:
        log_data['message'] = 'No new file generated, used FileRequest with ID {}'.format(file_request.file_request_id)
        logger.info(log_data)
    else:
        # Set SQS message attributes
        message_attr = {'agency_type': {'DataType': 'String', 'StringValue': agency_type}}
        if not job.submission_id:
            message_attr['agency_code'] = {'DataType': 'String', 'StringValue': agency_code}

        # Add job_id to the SQS job queue
        queue = sqs_queue()
        msg_response = queue.send_message(MessageBody=str(job.job_id), MessageAttributes=message_attr)

        log_data['message'] = 'SQS message response: {}'.format(msg_response)
        logger.debug(log_data)


def start_e_f_generation(job):
    """ Passes the Job ID for an E or F generation Job to SQS

        Args:
            job: File generation job to start
    """
    mark_job_status(job.job_id, "waiting")

    file_type = job.file_type.letter_name
    log_data = {'message': 'Sending {} file generation job {} to Validator in SQS'.format(file_type, job.job_id),
                'message_type': 'BrokerInfo', 'submission_id': job.submission_id, 'job_id': job.job_id,
                'file_type': file_type}
    logger.info(log_data)

    # Add job_id to the SQS job queue
    queue = sqs_queue()
    msg_response = queue.send_message(MessageBody=str(job.job_id), MessageAttributes={})

    log_data['message'] = 'SQS message response: {}'.format(msg_response)
    logger.debug(log_data)


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


def retrieve_cached_file_request(job, agency_type, agency_code, is_local=None):
    """ Retrieves a cached FileRequest for the D file generation, if there is one.

        Args:
            job: the upload job for the generation file
            agency_type: Type of Agency to generate files by: "awarding" or "funding"
            agency_code: Agency code for detached D file generations
            is_local: A boolean flag indicating whether the application is being run locally or not

        Returns:
            FileRequest object matching the criteria, or None
    """
    sess = GlobalDB.db().session
    log_data = {'message': 'Checking for a cached FileRequest to pull file from', 'message_type': 'BrokerInfo',
                'submission_id': job.submission_id, 'job_id': job.job_id, 'file_type': job.file_type.letter_name}
    logger.info(log_data)

    # find current date and date of last FPDS pull
    current_date = datetime.now().date()
    last_update = sess.query(FPDSUpdate).one_or_none()
    fpds_date = last_update.update_date if last_update else current_date

    # check if FileRequest already exists with this job_id, if not, create one
    file_request = None
    file_request_list = sess.query(FileRequest).filter(FileRequest.job_id == job.job_id).all()

    for fr in file_request_list:
        if (fr.file_type == 'D1' and fr.request_date < fpds_date) or fr.agency_type != agency_type or \
           fr.start_date != job.start_date or fr.end_date != job.end_date:
            # Uncache if D1 file is expired or old generation does not match current generation
            fr.is_cached_file = False
        else:
            file_request = fr
    sess.commit()

    # determine if anything needs to be done at all
    if file_request and file_request.is_cached_file:
        # this is the up-to-date cached version of the generated file
        # reset the file names on the upload Job
        log_data['message'] = '{} file has already been generated for this job'.format(file_request.file_type)
        logger.info(log_data)
        job.from_cached = False
        sess.commit()

        mark_job_status(job.job_id, 'finished')
    else:
        # search for potential parent FileRequests
        file_request = None
        parent_file_request = None
        parent_file_requests = sess.query(FileRequest).\
            filter(FileRequest.file_type == job.file_type.letter_name, FileRequest.start_date == job.start_date,
                   FileRequest.end_date == job.end_date, FileRequest.agency_code == agency_code,
                   FileRequest.agency_type == agency_type, FileRequest.is_cached_file.is_(True)).all()

        # there will, very rarely, be more than one value in parent_file_requests
        for parent_request in parent_file_requests:
            valid_cached_job_statuses = [lookups.JOB_STATUS_DICT["running"], lookups.JOB_STATUS_DICT["finished"]]
            parent_job = sess.query(Job).filter_by(job_id=parent_request.job_id).one_or_none()

            # check that D1 FileRequests are newer than the last FPDS pull
            invalid_d1 = parent_request.file_type == 'D1' and parent_request.request_date < fpds_date

            # check FileRequest hasn't expired and Job status is valid
            invalid_job = not parent_job or parent_job.job_status_id not in valid_cached_job_statuses

            # check that this parent_request is newer than any previous valid requests
            is_older_request = parent_file_request and parent_request.updated_at <= parent_file_request.updated_at

            # if this parent_request is not a valid cached FileRequest
            if invalid_d1 or invalid_job or is_older_request:
                # uncache FileRequest
                parent_request.is_cached_file = False
                continue

            # uncache outdated parent FileRequests
            if parent_file_request:
                parent_file_request.is_cached_file = False

            # mark FileRequest with parent job_id
            parent_file_request = parent_request

        sess.commit()

        if parent_file_request:
            # parent exists; copy parent data to this job
            log_data['message'] = 'Copying data for job_id:{} from parent job_id:{}'.format(job.job_id,
                                                                                            parent_file_request.job_id)
            logger.info(log_data)

            file_request = FileRequest(request_date=current_date, job_id=job.job_id, start_date=job.start_date,
                                       end_date=job.end_date, agency_code=agency_code, agency_type=agency_type,
                                       is_cached_file=False, file_type=job.file_type.letter_name,
                                       parent_job_id=parent_file_request.job_id)
            sess.add(file_request)
            sess.commit()

            copy_parent_file_request_data(job, parent_file_request.job, is_local)

    return file_request


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


def copy_parent_file_request_data(child_job, parent_job, is_local=None):
    """ Parent FileRequest job data to the child FileRequest job data.

        Args:
            child_job: Job object for the child FileRequest
            parent_job: Job object for the parent FileRequest
            is_local: A boolean flag indicating whether the application is being run locally or not
    """
    sess = GlobalDB.db().session

    # Do not edit submissions that have successfully completed
    if child_job.job_status_id == lookups.JOB_STATUS_DICT['finished']:
        return

    # Keep path but update file name
    filename = '{}/{}'.format(child_job.filename.rsplit('/', 1)[0], parent_job.original_filename)

    # Copy parent job's data
    child_job.from_cached = True
    child_job.filename = filename
    child_job.original_filename = parent_job.original_filename
    child_job.number_of_errors = parent_job.number_of_errors
    child_job.number_of_warnings = parent_job.number_of_warnings
    child_job.error_message = parent_job.error_message

    # Change the validation job's file data when within a submission
    if child_job.submission_id is not None:
        val_job = sess.query(Job).filter(Job.submission_id == child_job.submission_id,
                                         Job.file_type_id == parent_job.file_type_id,
                                         Job.job_type_id == lookups.JOB_TYPE_DICT['csv_record_validation']).one()
        val_job.filename = filename
        val_job.original_filename = parent_job.original_filename
    sess.commit()

    copy_file_from_parent_to_child(child_job, parent_job, is_local)

    # Mark job status last so the validation job doesn't start until everything is done
    mark_job_status(child_job.job_id, lookups.JOB_STATUS_DICT_ID[parent_job.job_status_id])


def copy_file_from_parent_to_child(child_job, parent_job, is_local):
    """ Copy the file from the parent job's bucket to the child job's bucket.

        Args:
            child_job: Job object for the child FileRequest
            parent_job: Job object for the parent FileRequest
            is_local: A boolean flag indicating whether the application is being run locally or not

    """
    file_type = parent_job.file_type.letter_name
    log_data = {'message': 'Copying data from parent job with job_id:{}'.format(parent_job.job_id),
                'message_type': 'ValidatorInfo', 'job_id': child_job.job_id, 'file_type': parent_job.file_type.name}

    if not is_local:
        is_local = g.is_local
    if not is_local and parent_job.filename != child_job.filename:
        # Check to see if the same file exists in the child bucket
        s3 = boto3.client('s3', region_name=CONFIG_BROKER["aws_region"])
        response = s3.list_objects_v2(Bucket=CONFIG_BROKER['aws_bucket'], Prefix=child_job.filename)
        for obj in response.get('Contents', []):
            if obj['Key'] == child_job.filename:
                # The file already exists in this location
                log_data['message'] = 'Cached {} file CSV already exists in this location'.format(file_type)
                logger.info(log_data)
                return

        # Copy the parent file into the child's S3 location
        log_data['message'] = 'Copying the cached {} file from job {}'.format(file_type, parent_job.job_id)
        logger.info(log_data)
        with smart_open.smart_open(S3Handler.create_file_path(parent_job.filename), 'r') as reader:
            stream_file_to_s3(child_job.filename, reader)


def update_validation_job_info(sess, job):
    """ Populates validation job objects with start and end dates, filenames, and status.
        Assumes the upload Job's start and end dates have been validated.
    """
    # Retrieve and update the validation Job
    val_job = sess.query(Job).filter(Job.submission_id == job.submission_id,
                                     Job.file_type_id == job.file_type_id,
                                     Job.job_type_id == lookups.JOB_TYPE_DICT['csv_record_validation']).one()
    val_job.start_date = job.start_date
    val_job.end_date = job.end_date
    val_job.filename = job.filename
    val_job.original_filename = job.original_filename
    val_job.job_status_id = lookups.JOB_STATUS_DICT["waiting"]

    # Clear out error messages to prevent stale messages
    job.error_message = None
    val_job.error_message = None

    sess.commit()


def d_file_query(query_utils, page_start, page_end):
    """ Retrieve D1 or D2 data.

        Args:
            query_utils: object containing:
                file_utils: fileD1 or fileD2 utils
                sess: database session
                agency_code: FREC or CGAC code for generation
                start: beginning of period for D file
                end: end of period for D file
            page_start: beginning of pagination
            page_end: end of pagination

        Return:
            paginated D1 or D2 query results
    """
    rows = query_utils["file_utils"].query_data(query_utils["sess"], query_utils["agency_code"],
                                                query_utils["agency_type"], query_utils["start"], query_utils["end"],
                                                page_start, page_end)
    return rows.all()
