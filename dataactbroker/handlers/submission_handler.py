import logging

from datetime import datetime
from flask import g
from sqlalchemy import func, or_, desc

from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import (sum_number_of_errors_for_job_list, get_last_validated_date,
                                                 get_fabs_meta, get_error_type, get_error_metrics_by_job_id)

from dataactcore.models.lookups import (JOB_STATUS_DICT, PUBLISH_STATUS_DICT, JOB_TYPE_DICT, RULE_SEVERITY_DICT,
                                        FILE_TYPE_DICT)
from dataactcore.models.domainModels import CGAC, FREC
from dataactcore.models.jobModels import (Job, Submission, SubmissionSubTierAffiliation, SubmissionWindow,
                                          CertifyHistory, RevalidationThreshold)
from dataactcore.models.stagingModels import (Appropriation, ObjectClassProgramActivity, AwardFinancial,
                                              CertifiedAppropriation, CertifiedObjectClassProgramActivity,
                                              CertifiedAwardFinancial)
from dataactcore.models.errorModels import File

from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.stringCleaner import StringCleaner

logger = logging.getLogger(__name__)


def create_submission(user_id, submission_values, existing_submission):
    """ Create a new submission if one doesn't exist, otherwise update the existing one

        Args:
            user_id:  user to associate with this submission
            submission_values: metadata about the submission
            existing_submission: id of existing submission (blank for new submissions)

        Returns:
            submission object
    """
    if existing_submission is None:
        submission = Submission(created_at=datetime.utcnow(), **submission_values)
        submission.user_id = user_id
        submission.publish_status_id = PUBLISH_STATUS_DICT['unpublished']
    else:
        submission = existing_submission
        if submission.publish_status_id == PUBLISH_STATUS_DICT['published']:
            submission.publish_status_id = PUBLISH_STATUS_DICT['updated']
        # submission is being updated, so turn off publishable flag
        submission.publishable = False
        for key in submission_values:
            # update existing submission with any values provided
            setattr(submission, key, submission_values[key])

    return submission


def populate_submission_error_info(submission_id):
    """ Set number of errors and warnings for submission.

        Args:
            submission_id: submission to update submission error info

        Returns:
            submission object
    """
    sess = GlobalDB.db().session
    submission = sess.query(Submission).filter(Submission.submission_id == submission_id).one()
    submission.number_of_errors = sum_number_of_errors_for_job_list(submission_id)
    submission.number_of_warnings = sum_number_of_errors_for_job_list(submission_id, error_type='warning')
    sess.commit()

    return submission


def get_submission_stats(submission_id):
    """ Get summarized dollar amounts by submission.

        Args:
            submission_id: submission to retrieve info from

        Returns:
            object containing total_obligations, total_procurement_obligations, total_assistance_obligations
    """
    sess = GlobalDB.db().session
    base_query = sess.query(func.sum(AwardFinancial.transaction_obligated_amou)).\
        filter(AwardFinancial.submission_id == submission_id)
    procurement = base_query.filter(AwardFinancial.piid.isnot(None))
    fin_assist = base_query.filter(or_(AwardFinancial.fain.isnot(None), AwardFinancial.uri.isnot(None)))
    return {
        "total_obligations": float(base_query.scalar() or 0),
        "total_procurement_obligations": float(procurement.scalar() or 0),
        "total_assistance_obligations": float(fin_assist.scalar() or 0)
    }


def get_submission_metadata(submission):
    """ Get metadata for the submission specified

        Args:
            submission: submission to retrieve metadata for

        Returns:
            object containing metadata for the submission
    """
    sess = GlobalDB.db().session

    # Determine the agency name
    agency_name = ''

    cgac = sess.query(CGAC).filter_by(cgac_code=submission.cgac_code).one_or_none()
    if cgac:
        agency_name = cgac.agency_name
    else:
        frec = sess.query(FREC).filter_by(frec_code=submission.frec_code).one_or_none()

        if frec:
            agency_name = frec.agency_name

    # Get the last validated date of the submission
    last_validated = get_last_validated_date(submission.submission_id)

    # Get metadata for FABS submissions
    fabs_meta = get_fabs_meta(submission.submission_id) if submission.d2_submission else None

    number_of_rows = sess.query(func.sum(Job.number_of_rows)).\
        filter_by(submission_id=submission.submission_id).\
        scalar() or 0

    total_size = sess.query(func.sum(Job.file_size)).\
        filter_by(submission_id=submission.submission_id).\
        scalar() or 0

    return {
        'cgac_code': submission.cgac_code,
        'frec_code': submission.frec_code,
        'agency_name': agency_name,
        'number_of_errors': submission.number_of_errors,
        'number_of_warnings': submission.number_of_warnings,
        'number_of_rows': number_of_rows,
        'total_size': total_size,
        'created_on': submission.created_at.strftime('%m/%d/%Y'),
        'last_updated': submission.updated_at.strftime("%Y-%m-%dT%H:%M:%S"),
        'last_validated': last_validated,
        'reporting_period': reporting_date(submission),
        'publish_status': submission.publish_status.name,
        'quarterly_submission': submission.is_quarter_format,
        'fabs_submission': submission.d2_submission,
        'fabs_meta': fabs_meta
    }


def get_submission_data(submission, file_type=''):
    """ Get data for the submission specified

        Args:
            submission: submission to retrieve metadata for
            file_type: the type of job to retrieve metadata for

        Returns:
            JsonResponse containing the error information or the object containing metadata for all relevant file types
    """
    sess = GlobalDB.db().session
    file_type = file_type.lower()

    # Make sure the file type provided is valid
    if file_type and file_type not in FILE_TYPE_DICT and file_type != 'cross':
        return JsonResponse.error(ValueError(file_type + ' is not a valid file type'), StatusCode.CLIENT_ERROR)

    # Make sure the file type provided is valid for the submission type
    is_fabs = submission.d2_submission
    if file_type and (is_fabs and file_type != 'fabs') or (not is_fabs and file_type == 'fabs'):
        return JsonResponse.error(ValueError(file_type + ' is not a valid file type for this submission'),
                                  StatusCode.CLIENT_ERROR)

    job_query = sess.query(Job).filter(Job.submission_id == submission.submission_id)
    if not file_type:
        relevant_job_types = (JOB_TYPE_DICT['csv_record_validation'], JOB_TYPE_DICT['validation'])
        job_query = job_query.filter(Job.job_type_id.in_(relevant_job_types))
    elif file_type == 'cross':
        job_query = job_query.filter(Job.job_type_id == JOB_TYPE_DICT['validation'])
    else:
        job_query = job_query.filter(Job.file_type_id == FILE_TYPE_DICT[file_type])

    job_dict = {'jobs': [job_to_dict(job) for job in job_query]}
    return JsonResponse.create(StatusCode.OK, job_dict)


def get_revalidation_threshold():
    """ Get the revalidation threshold for all submissions

        Returns:
            An object containing the revalidation threshold for submissions formatted in MM/DD/YYYY format
    """
    sess = GlobalDB.db().session
    reval_thresh = sess.query(RevalidationThreshold).one_or_none()

    return {
        'revalidation_threshold': reval_thresh.revalidation_date.strftime("%Y-%m-%dT%H:%M:%S") if reval_thresh else ''
    }


def reporting_date(submission):
    """ Format submission reporting date in MM/YYYY format for monthly submissions and Q#/YYYY for quarterly

        Args:
            submission: submission whose dates to format

        Returns:
            Formatted dates in the format specified above
    """
    if not (submission.reporting_start_date or submission.reporting_end_date):
        return None
    if submission.is_quarter_format:
        return 'Q{}/{}'.format(submission.reporting_fiscal_period // 3, submission.reporting_fiscal_year)
    else:
        return submission.reporting_start_date.strftime("%m/%Y")


def job_to_dict(job):
    """ Convert a Job model into a dictionary, ready to be serialized as JSON

        Args:
            job: job to convert into a dictionary

        Returns:
            A dictionary of job information
    """
    sess = GlobalDB.db().session

    job_info = {
        'job_id': job.job_id,
        'job_status': job.job_status_name,
        'job_type': job.job_type_name,
        'filename': job.original_filename,
        'file_size': job.file_size,
        'number_of_rows': job.number_of_rows,
        'file_type': job.file_type_name or ''
    }

    # @todo replace with relationships
    file_results = sess.query(File).filter_by(job_id=job.job_id).one_or_none()
    if file_results is None:
        # Job ID not in error database, probably did not make it to validation, or has not yet been validated
        job_info.update({
            'file_status': "",
            'error_type': "",
            'error_data': [],
            'warning_data': [],
            'missing_headers': [],
            'duplicated_headers': []
        })
    else:
        # If job ID was found in file, we should be able to get header error lists and file data. Get string of missing
        # headers and parse as a list
        job_info['file_status'] = file_results.file_status_name
        job_info['missing_headers'] = StringCleaner.split_csv(file_results.headers_missing)
        job_info["duplicated_headers"] = StringCleaner.split_csv(file_results.headers_duplicated)
        job_info["error_type"] = get_error_type(job.job_id)
        job_info["error_data"] = get_error_metrics_by_job_id(job.job_id, job.job_type_name == 'validation',
                                                             severity_id=RULE_SEVERITY_DICT['fatal'])
        job_info["warning_data"] = get_error_metrics_by_job_id(job.job_id, job.job_type_name == 'validation',
                                                               severity_id=RULE_SEVERITY_DICT['warning'])
    return job_info


def get_submission_status(submission, jobs):
    """ Return the status of a submission.

        Args:
            submission: submission to retrieve status from
            jobs: jobs within the submission to retrieve status from

        Returns:
            string containing the status of the submission
    """
    status_names = JOB_STATUS_DICT.keys()
    statuses = {name: 0 for name in status_names}

    for job in jobs:
        job_status = job.job_status.name
        statuses[job_status] += 1

    status = "unknown"

    if statuses["failed"] != 0:
        status = "failed"
    elif statuses["invalid"] != 0:
        status = "file_errors"
    elif statuses["running"] != 0:
        status = "running"
    elif statuses["waiting"] != 0:
        status = "waiting"
    elif statuses["ready"] != 0:
        status = "ready"
    elif statuses["finished"] == jobs.count():
        status = "validation_successful"
        if submission.number_of_warnings is not None and submission.number_of_warnings > 0:
            status = "validation_successful_warnings"
        if submission.publish_status_id == PUBLISH_STATUS_DICT['published']:
            status = "certified"

    # Check if submission has errors
    if submission.number_of_errors is not None and submission.number_of_errors > 0:
        status = "validation_errors"

    return status


def get_submission_files(jobs):
    """ Return the filenames of all jobs within a submission.

        Args:
            jobs: jobs to retrieve filenames from

        Returns:
            array of all filenames within the jobs given
    """
    job_list = []
    for job in jobs:
        if job.filename not in job_list:
            job_list.append(job.filename)
    return job_list


def delete_all_submission_data(submission):
    """ Delete a submission.

        Args:
            submission: submission to delete

        Returns:
            JsonResponse object containing a success message or the reason for failure
    """
    # check if the submission has been published, if so, do not allow deletion
    if submission.publish_status_id != PUBLISH_STATUS_DICT['unpublished']:
        return JsonResponse.error(ValueError("Submissions that have been certified cannot be deleted"),
                                  StatusCode.CLIENT_ERROR)

    sess = GlobalDB.db().session

    # check if the submission has any jobs that are currently running, if so, do not allow deletion
    running_jobs = sess.query(Job).filter(Job.submission_id == submission.submission_id,
                                          Job.job_status_id == JOB_STATUS_DICT['running']).all()
    if running_jobs:
        return JsonResponse.error(ValueError("Submissions with running jobs cannot be deleted"),
                                  StatusCode.CLIENT_ERROR)

    logger.info({
        "message": "Deleting submission with id {}".format(submission.submission_id),
        "message_type": "BrokerInfo",
        "submission_id": submission.submission_id
    })

    sess.query(SubmissionSubTierAffiliation).\
        filter(SubmissionSubTierAffiliation.submission_id == submission.submission_id).\
        delete(synchronize_session=False)
    sess.query(Submission).filter(Submission.submission_id == submission.submission_id).\
        delete(synchronize_session=False)

    sess.expire_all()

    return JsonResponse.create(StatusCode.OK, {"message": "Success"})


def list_windows():
    """ List all the windows (submission or otherwise) that are open at the time this is called.

        Returns:
            A JsonResponse containing data for each currently open window including start_date, end_date,
            notice_block, message, and type
    """
    current_windows = get_windows()

    data = []

    if current_windows.count() is 0:
        data = None
    else:
        for window in current_windows:
            data.append({
                'start_date': str(window.start_date),
                'end_date': str(window.end_date),
                'notice_block': window.block_certification,
                'message': window.message,
                'type': window.application_type.application_name
            })

    return JsonResponse.create(StatusCode.OK, {"data": data})


def get_windows():
    """ Get all submissions that start before and end after the current date

        Returns:
            A query to get all the windows surrounding the current date
    """
    sess = GlobalDB.db().session

    curr_date = datetime.now().date()

    return sess.query(SubmissionWindow).filter(SubmissionWindow.start_date <= curr_date,
                                               SubmissionWindow.end_date >= curr_date)


def check_current_submission_page(submission):
    """ Check what page of the submission the user should be allowed to see and if they should be redirected to an
        earlier one.

        Args:
            submission: The submission object

        Returns:
            A JsonResponse containing the step of the process the submission is on or that a step cannot be determined
    """
    sess = GlobalDB.db().session

    submission_id = submission.submission_id

    # /v1/uploadDetachedFiles/
    # DetachedFiles
    if submission.d2_submission:
        data = {
            "message": "The current progress of this submission ID is on /v1/uploadDetachedFiles/ page.",
            "step": "6"
        }
        return JsonResponse.create(StatusCode.OK, data)

    # /v1/reviewData/
    # Checks that both E and F files are finished
    review_data = sess.query(Job).filter(Job.submission_id == submission_id, Job.file_type_id.in_([6, 7]),
                                         Job.job_status_id == 4)

    # Need to check that cross file is done as well
    generate_ef = sess.query(Job).filter(Job.submission_id == submission_id, Job.job_type_id == 4,
                                         Job.number_of_errors == 0, Job.job_status_id == 4)

    if review_data.count() == 2 and generate_ef.count() > 0:
        data = {
            "message": "The current progress of this submission ID is on /v1/reviewData/ page.",
            "step": "5"
        }
        return JsonResponse.create(StatusCode.OK, data)

    # /v1/generateEF/
    if generate_ef.count() > 0:
        data = {
            "message": "The current progress of this submission ID is on /v1/generateEF/ page.",
            "step": "4"
        }
        return JsonResponse.create(StatusCode.OK, data)

    validate_cross_file = sess.query(Job).filter(Job.submission_id == submission_id, Job.file_type_id.in_([4, 5]),
                                                 Job.job_type_id == 2, Job.number_of_errors == 0,
                                                 Job.file_size.isnot(None), Job.job_status_id == 4)

    generate_files = sess.query(Job).filter(Job.submission_id == submission_id, Job.file_type_id.in_([1, 2, 3]),
                                            Job.job_type_id == 2, Job.number_of_errors == 0,
                                            Job.file_size.isnot(None), Job.job_status_id == 4)

    # /v1/validateCrossFile/
    if validate_cross_file.count() == 2 and generate_files.count() == 3:
        data = {
            "message": "The current progress of this submission ID is on /v1/validateCrossFile/ page.",
            "step": "3"
        }
        return JsonResponse.create(StatusCode.OK, data)

    # /v1/generateFiles/
    if generate_files.count() == 3:
        data = {
            "message": "The current progress of this submission ID is on /v1/generateFiles/ page.",
            "step": "2"
        }
        return JsonResponse.create(StatusCode.OK, data)

    # /v1/validateData/
    validate_data = sess.query(Job).filter(Job.submission_id == submission_id, Job.file_type_id.in_([1, 2, 3]),
                                           Job.job_type_id == 2, Job.number_of_errors != 0, Job.file_size.isnot(None))
    check_header_errors = sess.query(Job).filter(Job.submission_id == submission_id, Job.file_type_id.in_([1, 2, 3]),
                                                 Job.job_type_id == 2, Job.job_status_id != 4)
    if validate_data.count() or check_header_errors.count() > 0:
        data = {
            "message": "The current progress of this submission ID is on /v1/validateData/ page.",
            "step": "1"
        }
        return JsonResponse.create(StatusCode.OK, data)

    else:
        return JsonResponse.error(ValueError("The submission ID returns no response"), StatusCode.CLIENT_ERROR)


def find_existing_submissions_in_period(cgac_code, frec_code, reporting_fiscal_year, reporting_fiscal_period,
                                        submission_id=None):
    """ Find all the submissions in the given period for the given CGAC or FREC code

        Args:
            cgac_code: the CGAC code to check against or None if checking a FREC agency
            frec_code: the FREC code to check against or None if checking a CGAC agency
            reporting_fiscal_year: the year to check for
            reporting_fiscal_period: the period in the year to check for
            submission_id: the submission ID to check against (used when checking if this submission is being
                re-certified)

        Returns:
            A JsonResponse containing a success message to indicate there are no existing submissions in the given
            period or the error if there was one
    """
    # We need either a cgac or a frec code for this function
    if not cgac_code and not frec_code:
        return JsonResponse.error(ValueError("CGAC or FR Entity Code required"), StatusCode.CLIENT_ERROR)

    sess = GlobalDB.db().session

    submission_query = sess.query(Submission).filter(
        (Submission.cgac_code == cgac_code) if cgac_code else (Submission.frec_code == frec_code),
        Submission.reporting_fiscal_year == reporting_fiscal_year,
        Submission.reporting_fiscal_period == reporting_fiscal_period,
        Submission.publish_status_id != PUBLISH_STATUS_DICT['unpublished'])

    # Filter out the submission we are potentially re-certifying if one is provided
    if submission_id:
        submission_query = submission_query.filter(Submission.submission_id != submission_id)

    submission_query = submission_query.order_by(desc(Submission.created_at))

    if submission_query.count() > 0:
        data = {
            "message": "A submission with the same period already exists.",
            "submissionId": submission_query[0].submission_id
        }
        return JsonResponse.create(StatusCode.CLIENT_ERROR, data)
    return JsonResponse.create(StatusCode.OK, {"message": "Success"})


def move_certified_data(sess, submission_id):
    """ Move data from the staging tables to the certified tables for a submission.

        Args:
            sess: the database connection
            submission_id: The ID of the submission to move data for
    """
    table_types = {'appropriation': [Appropriation, CertifiedAppropriation],
                   'object_class_program_activity': [ObjectClassProgramActivity, CertifiedObjectClassProgramActivity],
                   'award_financial': [AwardFinancial, CertifiedAwardFinancial]}

    for table_type, table_object in table_types.items():
        logger.info({
            "message": "Deleting old certified data from {} table".format("certified_" + table_type),
            "message_type": "BrokerInfo",
            "submission_id": submission_id
        })

        # Delete the old certified data in the table
        sess.query(table_object[1]).filter_by(submission_id=submission_id).delete()

        logger.info({
            "message": "Moving certified data from {} table".format(table_type),
            "message_type": "BrokerInfo",
            "submission_id": submission_id
        })

        column_list = [col.key for col in table_object[0].__table__.columns]
        column_list.remove('created_at')
        column_list.remove('updated_at')
        column_list.remove(table_type + '_id')

        col_string = ", ".join(column_list)

        # Move the certified data
        sess.execute("INSERT INTO certified_{} (created_at, updated_at, {}) "
                     "SELECT NOW() AS created_at, NOW() AS updated_at, {} "
                     "FROM {} "
                     "WHERE submission_id={}".format(table_type, col_string, col_string, table_type, submission_id))


def certify_dabs_submission(submission, file_manager):
    """ Certify a DABS submission

        Args:
            submission: the submission to be certified
            file_manager: a FileHandler object to be used to call move_certified_files

        Returns:
            A JsonResponse containing the message "success" if successful, JsonResponse error containing the details of
            the error if something went wrong
    """
    current_user_id = g.user.user_id

    if not submission.publishable:
        return JsonResponse.error(ValueError("Submission cannot be certified due to critical errors"),
                                  StatusCode.CLIENT_ERROR)

    if not submission.is_quarter_format:
        return JsonResponse.error(ValueError("Monthly submissions cannot be certified"), StatusCode.CLIENT_ERROR)

    if submission.publish_status_id == PUBLISH_STATUS_DICT['published']:
        return JsonResponse.error(ValueError("Submission has already been certified"), StatusCode.CLIENT_ERROR)

    windows = get_windows()
    for window in windows:
        if window.block_certification:
            return JsonResponse.error(ValueError(window.message), StatusCode.CLIENT_ERROR)

    # check revalidation threshold
    sess = GlobalDB.db().session
    reval_thresh = get_revalidation_threshold()['revalidation_threshold']
    if reval_thresh and reval_thresh >= get_last_validated_date(submission.submission_id):
        return JsonResponse.error(ValueError("This submission has not been validated since before the revalidation "
                                             "threshold ({}), it must be revalidated before certifying.".
                                             format(reval_thresh.replace('T', ' '))),
                                  StatusCode.CLIENT_ERROR)

    response = find_existing_submissions_in_period(submission.cgac_code, submission.frec_code,
                                                   submission.reporting_fiscal_year,
                                                   submission.reporting_fiscal_period, submission.submission_id)

    if response.status_code == StatusCode.OK:
        # create the certify_history entry
        certify_history = CertifyHistory(created_at=datetime.utcnow(), user_id=current_user_id,
                                         submission_id=submission.submission_id)
        sess.add(certify_history)
        sess.commit()

        # get the certify_history entry including the PK
        certify_history = sess.query(CertifyHistory).filter_by(submission_id=submission.submission_id).\
            order_by(CertifyHistory.created_at.desc()).first()

        # Move the data to the certified table, deleting any old certified data in the process
        move_certified_data(sess, submission.submission_id)

        # move files (locally we don't move but we still need to populate the certified_files_history table)
        file_manager.move_certified_files(submission, certify_history, file_manager.is_local)

        # set submission contents
        submission.certifying_user_id = current_user_id
        submission.publish_status_id = PUBLISH_STATUS_DICT['published']
        sess.commit()

    return response
