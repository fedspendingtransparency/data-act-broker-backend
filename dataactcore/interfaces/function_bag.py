import logging
from operator import attrgetter
import time
import uuid
from datetime import datetime

from sqlalchemy import func, or_
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import NoResultFound

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import ExternalDataLoadDate
from dataactcore.models.errorModels import ErrorMetadata, File
from dataactcore.models.jobModels import (Job, Submission, JobDependency, PublishHistory, PublishedFilesHistory,
                                          SubmissionWindowSchedule)
from dataactcore.models.stagingModels import FABS
from dataactcore.models.userModel import User, EmailTemplateType, EmailTemplate
from dataactcore.models.validationModels import RuleSeverity
from dataactcore.models.views import SubmissionUpdatedView
from dataactcore.models.lookups import (FILE_TYPE_DICT, FILE_STATUS_DICT, JOB_TYPE_DICT,
                                        JOB_STATUS_DICT, FILE_TYPE_DICT_ID, PUBLISH_STATUS_DICT,
                                        EXTERNAL_DATA_TYPE_DICT)
from dataactcore.interfaces.db import GlobalDB
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactcore.aws.sqsHandler import sqs_queue


# This is a holding place for functions from a previous iteration of broker databases and database access code.
# Work still to do:
# - simplify functions
# - move functions to a better place?
# - replace GlobalDB function, which is deprecated now that db logic is refactored


logger = logging.getLogger(__name__)


# todo: move these value to config if it is decided to keep local user login long term
HASH_ROUNDS = 12


def create_user_with_password(email, password, bcrypt, website_admin=False):
    """Convenience function to set up fully-baked user (used for setup/testing only)."""
    sess = GlobalDB.db().session
    user = User(
        email=email, name='Administrator',
        title='System Admin', website_admin=website_admin
    )
    user.salt, user.password_hash = get_password_hash(password, bcrypt)
    sess.add(user)
    sess.commit()

    return user


def get_password_hash(password, bcrypt):
    """Generate password hash."""
    # TODO: handle password hashing/lookup in the User model
    salt = uuid.uuid4().hex
    # number 12 below iw the number of rounds for bcrypt
    encoded_hash = bcrypt.generate_password_hash(password + salt, HASH_ROUNDS)
    password_hash = encoded_hash.decode('utf-8')
    return salt, password_hash


def populate_job_error_info(job):
    """ Set number of errors and warnings for specified job. """
    sess = GlobalDB.db().session
    job.number_of_errors = sess.query(func.sum(ErrorMetadata.occurrences)).\
        join(ErrorMetadata.severity).\
        filter(ErrorMetadata.job_id == job.job_id, RuleSeverity.name == 'fatal').\
        scalar() or 0

    job.number_of_warnings = sess.query(func.sum(ErrorMetadata.occurrences)).\
        join(ErrorMetadata.severity).\
        filter(ErrorMetadata.job_id == job.job_id, RuleSeverity.name == 'warning').\
        scalar() or 0
    sess.commit()


def sum_number_of_errors_for_job_list(submission_id, error_type='fatal'):
    """Add number of errors for all jobs in list."""
    sess = GlobalDB.db().session
    error_sum = 0
    jobs = sess.query(Job).filter(Job.submission_id == submission_id).all()
    for job in jobs:
        if error_type == 'fatal':
            error_sum += job.number_of_errors
        elif error_type == 'warning':
            error_sum += job.number_of_warnings
    return error_sum


""" ERROR DB FUNCTIONS """


def get_error_type(job_id):
    """ Returns either "none", "header_errors", or "row_errors" depending on what errors occurred during validation """
    sess = GlobalDB.db().session
    file_status_name = sess.query(File).options(joinedload('file_status')).\
        filter(File.job_id == job_id).one().file_status.name
    if file_status_name == 'header_error':
        # Header errors occurred, return that
        return 'header_errors'
    elif sess.query(Job).filter(Job.job_id == job_id).one().number_of_errors > 0:
        # Row errors occurred
        return 'row_errors'
    else:
        # No errors occurred during validation
        return 'none'


def create_file_if_needed(job_id, filename=None):
    """ Return the existing file object if it exists, or create a new one """
    sess = GlobalDB.db().session
    try:
        file_rec = sess.query(File).filter(File.job_id == job_id).one()
        # Set new filename for changes to an existing submission
        file_rec.filename = filename
    except NoResultFound:
        file_rec = create_file(job_id, filename)
    return file_rec


def create_file(job_id, filename):
    """ Create a new file object for specified job and filename """
    sess = GlobalDB.db().session
    try:
        int(job_id)
    except Exception:
        logger.error({
            'message': 'Bad job_id: {}'.format(job_id),
            'message_type': 'CoreError',
            'job_id': job_id,
            'function': 'create_file'
        })
        raise ValueError('Bad job_id: {}'.format(job_id))

    file_rec = File(job_id=job_id, filename=filename, file_status_id=FILE_STATUS_DICT['incomplete'])
    sess.add(file_rec)
    sess.commit()
    return file_rec


def write_file_error(job_id, filename, error_type, extra_info=None):
    """ Write a file-level error to the file table

    Args:
        job_id: ID of job in job tracker
        filename: name of error report in S3
        error_type: type of error, value will be mapped to ValidationError class
        extra_info: list of extra information to be included in file
    """
    sess = GlobalDB.db().session
    try:
        int(job_id)
    except Exception:
        logger.error({
            'message': 'Bad job_id: {}'.format(job_id),
            'message_type': 'CoreError',
            'job_id': job_id,
            'function': 'write_file_error'
        })
        raise ValueError('Bad job_id: {}'.format(job_id))

    # Get File object for this job ID or create it if it doesn't exist
    file_rec = create_file_if_needed(job_id, filename)

    # Mark error type and add header info if present
    file_rec.file_status_id = FILE_STATUS_DICT[ValidationError.get_error_type_string(error_type)]
    if extra_info is not None:
        if 'missing_headers' in extra_info:
            file_rec.headers_missing = extra_info['missing_headers']
        if 'duplicated_headers' in extra_info:
            file_rec.headers_duplicated = extra_info['duplicated_headers']

    sess.add(file_rec)
    sess.commit()


def mark_file_complete(job_id, filename=None):
    """ Marks file's status as complete

    Args:
        job_id: ID of job in job tracker
        filename: name of error report in S3
    """
    sess = GlobalDB.db().session
    file_complete = create_file_if_needed(job_id, filename)
    file_complete.file_status_id = FILE_STATUS_DICT['complete']
    sess.commit()


def get_error_metrics_by_job_id(job_id, include_file_types=False, severity_id=None):
    """ Get error metrics for specified job, including number of errors for each field name and error type """
    sess = GlobalDB.db().session
    result_list = []

    query_result = sess.query(File).options(joinedload('file_status')).filter(File.job_id == job_id).one()

    if not query_result.file_status.file_status_id == FILE_STATUS_DICT['complete']:
        return [{'field_name': 'File Level Error', 'error_name': query_result.file_status.name,
                 'error_description': query_result.file_status.description, 'occurrences': 1, 'rule_failed': ''}]

    query_result = sess.query(ErrorMetadata).options(joinedload('error_type')).filter(
        ErrorMetadata.job_id == job_id, ErrorMetadata.severity_id == severity_id).all()
    for result in query_result:
        record_dict = {'field_name': result.field_name, 'error_name': result.error_type.name,
                       'error_description': result.error_type.description, 'occurrences': result.occurrences,
                       'rule_failed': result.rule_failed, 'original_label': result.original_rule_label}
        if include_file_types:
            record_dict['source_file'] = FILE_TYPE_DICT_ID.get(result.file_type_id, '')
            record_dict['target_file'] = FILE_TYPE_DICT_ID.get(result.target_file_type_id, '')
        result_list.append(record_dict)
    return result_list


def get_email_template(email_type):
    """ Get template for specified email type
    Arguments:
        email_type - Name of template to get
    Returns:
        EmailTemplate object
    """
    sess = GlobalDB.db().session
    type_result = sess.query(EmailTemplateType.email_template_type_id).\
        filter(EmailTemplateType.name == email_type).one()
    template_result = sess.query(EmailTemplate).\
        filter(EmailTemplate.template_type_id == type_result.email_template_type_id).one()
    return template_result


def check_correct_password(user, password, bcrypt):
    """ Given a user object and a password, verify that the password is correct.

    Arguments:
        user - User object
        password - Password to check
        bcrypt - bcrypt to use for password hashing
    Returns:
         True if valid password, False otherwise.
    """
    if password is None or password.strip() == "":
        # If no password or empty password, reject
        return False

    # Check the password with bcrypt
    return bcrypt.check_password_hash(user.password_hash, password + user.salt)


def run_job_checks(job_id):
    """ Checks that specified job has no unsatisfied prerequisites
    Args:
        job_id -- job_id of job to be run

    Returns:
        True if prerequisites are satisfied, False if not
    """
    sess = GlobalDB.db().session

    # Get count of job's prerequisites that are not yet finished
    incomplete_dependencies = sess.query(JobDependency). \
        join('prerequisite_job'). \
        filter(JobDependency.job_id == job_id, Job.job_status_id != JOB_STATUS_DICT['finished']). \
        count()
    if incomplete_dependencies:
        return False
    else:
        return True


def mark_job_status(job_id, status_name, skip_check=False):
    """
    Mark job as having specified status.
    Jobs being marked as finished will add dependent jobs to queue.

    Args:
        job_id: ID for job being marked
        status_name: Status to change job to
    """
    sess = GlobalDB.db().session

    job = sess.query(Job).filter(Job.job_id == job_id).one()
    old_status = job.job_status.name
    # update job status
    job.job_status_id = JOB_STATUS_DICT[status_name]
    if status_name in ('ready', 'waiting'):
        job.error_message = None
    sess.commit()

    # if status is changed to finished for the first time, check dependencies
    # and add to the job queue as necessary
    if old_status != 'finished' and status_name == 'finished' and not skip_check:
        check_job_dependencies(job_id)


def check_job_dependencies(job_id):
    """ For specified job, check which of its dependencies are ready to be started and add them to the queue

        Args:
            job_id: the ID of the job that was just finished

        Raises:
            ValueError: If the job provided is not finished
    """
    sess = GlobalDB.db().session
    log_data = {
        'message_type': 'CoreError',
        'job_id': job_id
    }

    # raise exception if current job is not actually finished
    job = sess.query(Job).filter(Job.job_id == job_id).one()
    if job.job_status_id != JOB_STATUS_DICT['finished']:
        log_data['message'] = 'Current job not finished, unable to check dependencies'
        logger.error(log_data)
        raise ValueError('Current job not finished, unable to check dependencies')

    # get the jobs that are dependent on job_id being finished
    dependencies = sess.query(JobDependency).filter_by(prerequisite_id=job_id).all()
    for dependency in dependencies:
        dep_job_id = dependency.job_id
        if dependency.dependent_job.job_status_id != JOB_STATUS_DICT['waiting']:
            log_data['message_type'] = 'CoreError'
            log_data['message'] = '{} (dependency of {}) is not in a \'waiting\' state'.format(dep_job_id, job_id)
            logger.error(log_data)
        else:
            # find the number of this job's prerequisites that do not have a status of 'finished' or have errors.
            unfinished_prerequisites = sess.query(JobDependency).\
                join(Job, JobDependency.prerequisite_job).\
                filter(or_(Job.job_status_id != JOB_STATUS_DICT['finished'], Job.number_of_errors > 0),
                       JobDependency.job_id == dep_job_id).\
                count()
            if unfinished_prerequisites == 0:
                # this job has no unfinished prerequisite jobs, so it is eligible to be set to a 'ready' status and
                # added to the queue
                mark_job_status(dep_job_id, 'ready')

                # Only want to send validation jobs to the queue, other job types should be forwarded
                if dependency.dependent_job.job_type_name in ['csv_record_validation', 'validation']:
                    # add dep_job_id to the SQS job queue
                    log_data['message_type'] = 'CoreInfo'
                    log_data['message'] = 'Sending job {} to job manager in sqs'.format(dep_job_id)
                    logger.info(log_data)
                    queue = sqs_queue()
                    response = queue.send_message(MessageBody=str(dep_job_id))
                    log_data['message'] = 'Send message response: {}'.format(response)
                    logger.info(log_data)


def create_jobs(upload_files, submission, existing_submission=False):
    """Create the set of jobs associated with the specified submission

    Arguments:
    upload_files -- list of named tuples that describe files uploaded to the broker
    submission -- submission
    existing_submission -- true if we should update jobs in an existing submission rather than creating new jobs

    Returns:
    Dictionary of upload ids by filename to return to client, used for calling finalize_submission route
    """
    sess = GlobalDB.db().session
    submission_id = submission.submission_id

    # create the file upload and single-file validation jobs and
    # set up the dependencies between them
    # before starting, sort the incoming list of jobs by letter
    # to ensure that jobs dependent on the awards jobs being present
    # are processed last.
    jobs_required = []
    upload_dict = {}
    sorted_uploads = sorted(upload_files, key=attrgetter('file_letter'))

    for upload_file in sorted_uploads:
        validation_job_id, upload_job_id = add_jobs_for_uploaded_file(upload_file, submission_id, existing_submission)
        if validation_job_id:
            jobs_required.append(validation_job_id)
        upload_dict[upload_file.file_type] = upload_job_id

    # once single-file upload/validation jobs are created, create the cross-file
    # validation job and dependencies
    if existing_submission and not submission.is_fabs:
        # find cross-file jobs and mark them as waiting
        # (note: job_type of 'validation' is a cross-file job)
        val_job = sess.query(Job).\
            filter_by(
                submission_id=submission_id,
                job_type_id=JOB_TYPE_DICT['validation']).\
            one()
        val_job.job_status_id = JOB_STATUS_DICT['waiting']
        submission.updated_at = time.strftime('%c')
    elif not submission.is_fabs:
        # create cross-file validation job
        validation_job = Job(
            job_status_id=JOB_STATUS_DICT['waiting'],
            job_type_id=JOB_TYPE_DICT['validation'],
            submission_id=submission_id)
        sess.add(validation_job)
        sess.flush()
        # create dependencies for validation jobs
        for job_id in jobs_required:
            val_dependency = JobDependency(job_id=validation_job.job_id, prerequisite_id=job_id)
            sess.add(val_dependency)

    sess.commit()
    upload_dict['submission_id'] = submission_id
    return upload_dict


def add_jobs_for_uploaded_file(upload_file, submission_id, existing_submission):
    """ Add upload and validation jobs for a single filetype

    Arguments:
        upload_file: UploadFile named tuple
        submission_id: submission ID to attach to jobs
        existing_submission: true if we should update existing jobs rather than creating new ones

    Returns:
        the validation job id for this file type (if any)
        the upload job id for this file type
    """
    sess = GlobalDB.db().session

    file_type_id = FILE_TYPE_DICT[upload_file.file_type]
    validation_job_id = None

    # Create a file upload job or, for an existing submission, modify the
    # existing upload job.

    if existing_submission:
        # mark existing upload job as running
        upload_job = sess.query(Job).filter_by(
            submission_id=submission_id,
            file_type_id=file_type_id,
            job_type_id=JOB_TYPE_DICT['file_upload']
        ).one()
        # mark as running and set new file name and path
        upload_job.job_status_id = JOB_STATUS_DICT['running']
        upload_job.original_filename = upload_file.file_name
        upload_job.progress = 0
        upload_job.filename = upload_file.upload_name
        upload_job.error_message = None

    else:
        if upload_file.file_type in ['award', 'award_procurement']:
            # file generation handled on backend, mark as ready
            upload_status = JOB_STATUS_DICT['ready']
        elif upload_file.file_type in ['executive_compensation', 'sub_award']:
            # these are dependent on file D2 validation
            upload_status = JOB_STATUS_DICT['waiting']
        else:
            # mark as running since frontend should be doing this upload
            upload_status = JOB_STATUS_DICT['running']

        upload_job = Job(
            original_filename=upload_file.file_name,
            filename=upload_file.upload_name,
            file_type_id=file_type_id,
            job_status_id=upload_status,
            job_type_id=JOB_TYPE_DICT['file_upload'],
            submission_id=submission_id)
        sess.add(upload_job)
        sess.flush()

    if existing_submission:
        # if the file's validation job is attached to an existing submission,
        # reset its status and delete any validation artifacts (e.g., error metadata) that
        # might exist from a previous run.
        val_job = sess.query(Job).filter_by(
            submission_id=submission_id,
            file_type_id=file_type_id,
            job_type_id=JOB_TYPE_DICT['csv_record_validation']
        ).one()
        val_job.job_status_id = JOB_STATUS_DICT['waiting']
        val_job.original_filename = upload_file.file_name
        val_job.filename = upload_file.upload_name
        val_job.progress = 0
        # reset file size and number of rows to be set during validation of new file
        val_job.file_size = None
        val_job.number_of_rows = None
        # delete error metadata this might exist from a previous run of this validation job
        sess.query(ErrorMetadata).\
            filter(ErrorMetadata.job_id == val_job.job_id).\
            delete(synchronize_session='fetch')
        # delete file error information that might exist from a previous run of this validation job
        sess.query(File).filter(File.job_id == val_job.job_id).delete(synchronize_session='fetch')

    else:
        # create a new record validation job and add dependencies if necessary
        if upload_file.file_type == 'executive_compensation':
            d1_val_job = sess.query(Job).\
                filter(Job.submission_id == submission_id,
                       Job.file_type_id == FILE_TYPE_DICT['award_procurement'],
                       Job.job_type_id == JOB_TYPE_DICT['csv_record_validation']).\
                one_or_none()
            if d1_val_job is None:
                logger.error({
                    'message': 'Cannot create E job without a D1 job',
                    'message_type': 'CoreError',
                    'submission_id': submission_id,
                    'file_type': 'E'
                })
                raise Exception('Cannot create E job without a D1 job')
            # Add dependency on D1 validation job
            d1_dependency = JobDependency(job_id=upload_job.job_id, prerequisite_id=d1_val_job.job_id)
            sess.add(d1_dependency)

        elif upload_file.file_type == 'sub_award':
            # todo: check for C validation job
            c_val_job = sess.query(Job).\
                filter(Job.submission_id == submission_id,
                       Job.file_type_id == FILE_TYPE_DICT['award_financial'],
                       Job.job_type_id == JOB_TYPE_DICT['csv_record_validation']).\
                one_or_none()
            if c_val_job is None:
                logger.error({
                    'message': 'Cannot create F job without a C job',
                    'message_type': 'CoreError',
                    'submission_id': submission_id,
                    'file_type': 'F'
                })
                raise Exception('Cannot create F job without a C job')
            # add dependency on C validation job
            c_dependency = JobDependency(job_id=upload_job.job_id, prerequisite_id=c_val_job.job_id)
            sess.add(c_dependency)

        else:
            # E and F don't get validation jobs
            val_job = Job(
                original_filename=upload_file.file_name,
                filename=upload_file.upload_name,
                file_type_id=file_type_id,
                job_status_id=JOB_STATUS_DICT['waiting'],
                job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                submission_id=submission_id)
            sess.add(val_job)
            sess.flush()
            # add dependency between file upload job and file validation job
            upload_dependency = JobDependency(job_id=val_job.job_id, prerequisite_id=upload_job.job_id)
            sess.add(upload_dependency)
            validation_job_id = val_job.job_id

    sess.commit()

    return validation_job_id, upload_job.job_id


def get_latest_published_date(submission, is_fabs=False):
    if submission.publish_status_id != PUBLISH_STATUS_DICT['unpublished'] and\
       submission.publish_status_id != PUBLISH_STATUS_DICT['publishing']:
        sess = GlobalDB.db().session
        last_published = sess.query(PublishHistory).filter_by(submission_id=submission.submission_id).\
            order_by(PublishHistory.created_at.desc()).first()

        published_files = None
        if is_fabs:
            published_files = sess.query(PublishedFilesHistory).\
                filter_by(publish_history_id=last_published.publish_history_id).first()

        if last_published and published_files:
            return last_published.created_at, published_files.filename
        elif last_published:
            return last_published.created_at
    return None


def get_certification_deadline(submission):
    """ Return the certification deadline for the given submission

        Arguments:
            submission: the submission object to find its end window

        Returns:
            the datetime of the submission's window end
    """
    sess = GlobalDB.db().session
    cert_deadline = None
    if not submission.is_fabs:
        sub_period = submission.reporting_fiscal_period
        sub_year = submission.reporting_fiscal_year
        sub_window = sess.query(SubmissionWindowSchedule).filter_by(year=sub_year, period=sub_period).\
            one_or_none()
        cert_deadline = sub_window.certification_deadline.date() if sub_window else None
    return cert_deadline


def get_time_period(submission):
    """ Return the time period for the given submission

        Arguments:
            submission: the submission object to find its end window

        Returns:
            the time period of the submission
    """
    if not submission.is_fabs and submission.is_quarter_format:
        sub_quarter = submission.reporting_fiscal_period // 3
        sub_year = submission.reporting_fiscal_year
        time_period = 'FY {} / Q{}'.format(str(sub_year)[2:], sub_quarter)
    else:
        time_period = submission.reporting_start_date.strftime('%m / %Y') if submission.reporting_start_date else ''
    return time_period


def filename_fyp_sub_format(submission):
    """ Wrapper for filename_fyp_format that takes in a submission object (must have the necessary fields)

        Arguments:
            submission: the submission object to find the time period

        Returns:
            the submission's time period string for filenames
    """
    return filename_fyp_format(submission.reporting_fiscal_year, submission.reporting_fiscal_period,
                               submission.is_quarter_format)


def filename_fyp_format(fy, period, is_quarter):
    """ Return the proper FYP string to be included in the filenames throughout Broker

        Arguments:
            fy: fiscal year
            period: the period
            is_quarter: whether it should be based on quarters or periods

        Returns:
            the time period string for filenames
    """
    if is_quarter:
        suffix = 'Q{}'.format(int(period) // 3)
    elif int(period) > 2:
        suffix = 'P{}'.format(str(period).zfill(2))
    else:
        suffix = 'P01-P02'
    return 'FY{}{}'.format(str(fy)[2:], suffix)


def get_last_validated_date(submission_id):
    """ Return the oldest last validated date for validation jobs """
    sess = GlobalDB.db().session

    validation_job_types = [JOB_TYPE_DICT['csv_record_validation'], JOB_TYPE_DICT['validation']]

    jobs = sess.query(Job).filter(Job.submission_id == submission_id,
                                  Job.job_type_id.in_(validation_job_types)).all()

    oldest_date = ''
    for job in jobs:
        # if any job's last validated doesn't exist, return blank immediately
        if not job.last_validated:
            return ''

        if not oldest_date or job.last_validated < oldest_date:
            oldest_date = job.last_validated

    # Still need to do a check here in case there aren't any jobs for a submission.
    # This is the case for a single unit test
    return oldest_date.strftime('%Y-%m-%dT%H:%M:%S') if oldest_date else oldest_date


def get_fabs_meta(submission_id):
    """Return the total rows, valid rows, publish date, and publish file for FABS submissions"""
    sess = GlobalDB.db().session

    # get row counts from the FABS table
    total_rows = sess.query(FABS).filter(FABS.submission_id == submission_id)
    valid_rows = total_rows.filter(FABS.is_valid)

    # retrieve the published data and file
    submission = sess.query(Submission).filter(Submission.submission_id == submission_id).one()
    publish_date, published_file = None, None
    publish_data = get_latest_published_date(submission, is_fabs=True)

    try:
        iter(publish_data)
    except TypeError:
        publish_date = publish_data
    else:
        publish_date, file_path = publish_data
        if CONFIG_BROKER['use_aws'] and file_path:
            path, file_name = file_path.rsplit('/', 1)  # split by last instance of /
            published_file = S3Handler().get_signed_url(path=path, file_name=file_name,
                                                        bucket_route=CONFIG_BROKER['certified_bucket'],
                                                        url_mapping=CONFIG_BROKER['certified_bucket_mapping'])
        elif file_path:
            published_file = file_path

    return {
        'valid_rows': valid_rows.count(),
        'total_rows': total_rows.count(),
        'publish_date': publish_date.strftime('%Y-%m-%dT%H:%M:%S') if publish_date else None,
        'published_file': published_file
    }


def get_action_dates(submission_id):
    """ Pull the earliest/latest action dates from the FABS table

        Args:
            submission_id: submission ID pull action dates from

        Returns:
            the earliest action date (str) or None if not found
            the latest action date (str) or None if not found
    """

    sess = GlobalDB.db().session
    return sess.query(func.min(FABS.action_date).label('min_action_date'),
                      func.max(FABS.action_date).label('max_action_date'))\
        .filter(FABS.submission_id == submission_id,
                FABS.is_valid.is_(True)).one()


def get_last_modified(submission_id):
    """ Get the last modified date for a submission

        Args:
            submission_id: submission ID to get the last modified for

        Returns:
            the last modified date of the provided submission or None if the submission doesn't exist
    """
    submission_updated_view = SubmissionUpdatedView()
    sess = GlobalDB.db().session
    last_modified = sess.query(submission_updated_view.updated_at).\
        filter(submission_updated_view.submission_id == submission_id).first()
    return last_modified.updated_at if last_modified else None


def get_timestamp():
    """ Gets a timestamp in seconds

        Returns:
            a string representing seconds since the epoch
    """
    return str(int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds()))


def update_external_data_load_date(start_time, end_time, data_type):
    """ Update the external_data_load_date table with the start and end times for the given data type

        Args:
            start_time: a datetime object indicating the start time of the external data load
            end_time: a datetime object indicating the end time of the external data load
            data_type: a string indicating the data type of the external data load
    """
    sess = GlobalDB.db().session
    last_stored_obj = sess.query(ExternalDataLoadDate).\
        filter_by(external_data_type_id=EXTERNAL_DATA_TYPE_DICT[data_type]).one_or_none()
    if not last_stored_obj:
        last_stored_obj = ExternalDataLoadDate(
            external_data_type_id=EXTERNAL_DATA_TYPE_DICT[data_type],
            last_load_date_start=start_time, last_load_date_end=end_time)
        sess.add(last_stored_obj)
    else:
        last_stored_obj.last_load_date_start = start_time
        last_stored_obj.last_load_date_end = end_time
    sess.commit()
