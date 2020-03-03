import logging
import os

from datetime import datetime
from flask import g
from sqlalchemy import func, or_, desc
from sqlalchemy.sql.expression import case

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import (sum_number_of_errors_for_job_list, get_last_validated_date,
                                                 get_fabs_meta, get_error_type, get_error_metrics_by_job_id)

from dataactcore.models.lookups import (JOB_STATUS_DICT, PUBLISH_STATUS_DICT, JOB_TYPE_DICT, RULE_SEVERITY_DICT,
                                        FILE_TYPE_DICT)
from dataactcore.models.errorModels import ErrorMetadata, CertifiedErrorMetadata
from dataactcore.models.domainModels import CGAC, FREC
from dataactcore.models.jobModels import (Job, Submission, SubmissionSubTierAffiliation, SubmissionWindow,
                                          CertifyHistory, RevalidationThreshold, QuarterlyRevalidationThreshold,
                                          Comment, CertifiedComment)
from dataactcore.models.stagingModels import (Appropriation, ObjectClassProgramActivity, AwardFinancial,
                                              CertifiedAppropriation, CertifiedObjectClassProgramActivity,
                                              CertifiedAwardFinancial, FlexField, CertifiedFlexField, AwardProcurement,
                                              AwardFinancialAssistance, CertifiedAwardProcurement,
                                              CertifiedAwardFinancialAssistance)
from dataactcore.models.errorModels import File

from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
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

    # We need to ignore one row from each job for the header
    number_of_rows = sess.query(func.sum(case([(Job.number_of_rows > 0, Job.number_of_rows-1)], else_=0))).\
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


def get_latest_certification_period():
    """ Get the latest quarterly certification period for all submissions

        Returns:
            A dictionary containing the latest certification period (quarter and year)
    """
    sess = GlobalDB.db().session
    last_cert_period = sess.query(QuarterlyRevalidationThreshold.quarter, QuarterlyRevalidationThreshold.year).\
        filter(QuarterlyRevalidationThreshold.window_start <= datetime.today()).\
        order_by(QuarterlyRevalidationThreshold.window_start.desc()).first()
    return {
        'quarter': last_cert_period.quarter if last_cert_period else None,
        'year': last_cert_period.year if last_cert_period else None
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
        'number_of_rows': job.number_of_rows - 1 if job.number_of_rows else 0,
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
                'header': window.header,
                'message': window.message,
                'type': window.application_type.application_name,
                'banner_type': window.banner_type
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


def move_certified_data(sess, submission_id, direction='certify'):
    """ Move data from the staging tables to the certified tables for a submission or do the reverse for a revert.

        Args:
            sess: the database connection
            submission_id: The ID of the submission to move data for
            direction: The direction to move the certified data (certify or revert)
    """
    table_types = {'appropriation': [Appropriation, CertifiedAppropriation, 'submission'],
                   'object_class_program_activity': [ObjectClassProgramActivity, CertifiedObjectClassProgramActivity,
                                                     'submission'],
                   'award_financial': [AwardFinancial, CertifiedAwardFinancial, 'submission'],
                   'award_procurement': [AwardProcurement, CertifiedAwardProcurement, 'submission'],
                   'award_financial_assistance': [AwardFinancialAssistance, CertifiedAwardFinancialAssistance,
                                                  'submission'],
                   'error_metadata': [ErrorMetadata, CertifiedErrorMetadata, 'job'],
                   'comment': [Comment, CertifiedComment, 'submission'],
                   'flex_field': [FlexField, CertifiedFlexField, 'submission']}

    # Get list of jobs so we can use them for filtering
    job_list = sess.query(Job.job_id).filter_by(submission_id=submission_id).all()
    job_list = [item[0] for item in job_list]

    for table_type, table_object in table_types.items():
        if direction == 'certify':
            source_table = table_object[0]
            target_table = table_object[1]
        else:
            source_table = table_object[1]
            target_table = table_object[0]

        logger.info({
            'message': 'Deleting old data from {} table'.format(source_table.__table__.name),
            'message_type': 'BrokerInfo',
            'submission_id': submission_id
        })

        # Delete the old data in the target table
        if table_object[2] == 'submission':
            sess.query(target_table).filter_by(submission_id=submission_id).delete(synchronize_session=False)
        else:
            sess.query(target_table).filter(target_table.job_id.in_(job_list)).delete(synchronize_session=False)

        logger.info({
            'message': 'Moving certified data from {} table'.format(source_table.__table__.name),
            'message_type': 'BrokerInfo',
            'submission_id': submission_id
        })

        column_list = [col.key for col in table_object[0].__table__.columns]
        column_list.remove('created_at')
        column_list.remove('updated_at')
        column_list.remove(table_type + '_id')

        col_string = ', '.join(column_list)

        insert_string = """
            INSERT INTO {target} (created_at, updated_at, {cols})
            SELECT NOW() AS created_at, NOW() AS updated_at, {cols}
            FROM {source}
            WHERE
        """.format(source=source_table.__table__.name, target=target_table.__table__.name, cols=col_string)

        # Filter by either submission ID or job IDs depending on the situation
        if table_object[2] == 'submission':
            insert_string += ' submission_id={}'.format(submission_id)
        else:
            insert_string += ' job_id IN ({})'.format(','.join(str(job) for job in job_list))

        # Move the certified data
        sess.execute(insert_string)


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

    sess = GlobalDB.db().session
    # Check revalidation threshold
    last_validated = get_last_validated_date(submission.submission_id)
    reval_thresh = get_revalidation_threshold()['revalidation_threshold']
    if reval_thresh and reval_thresh >= last_validated:
        return JsonResponse.error(ValueError("This submission has not been validated since before the revalidation "
                                             "threshold ({}), it must be revalidated before certifying.".
                                             format(reval_thresh.replace('T', ' '))),
                                  StatusCode.CLIENT_ERROR)

    # Get the year/quarter of the submission and filter by them
    sub_quarter = submission.reporting_fiscal_period // 3
    sub_year = submission.reporting_fiscal_year
    quarter_reval = sess.query(QuarterlyRevalidationThreshold).filter_by(year=sub_year, quarter=sub_quarter).\
        one_or_none()

    # If we don't have a quarterly revalidation threshold for this year/quarter, they can't submit
    if not quarter_reval:
        return JsonResponse.error(ValueError("No submission window for this year and quarter was found. If this is an "
                                             "error, please contact the Service Desk."), StatusCode.CLIENT_ERROR)

    # Make sure everything was last validated after the start of the submission window
    last_validated = datetime.strptime(last_validated, '%Y-%m-%dT%H:%M:%S')
    if last_validated < quarter_reval.window_start:
        return JsonResponse.error(ValueError("This submission was last validated or its D files generated before the "
                                             "start of the submission window ({}). Please revalidate before "
                                             "certifying.".format(quarter_reval.window_start.strftime('%m/%d/%Y'))),
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


def revert_to_certified(submission, file_manager):
    """ Revert an updated DABS submission to its last certified state

        Args:
            submission: the submission to be reverted
            file_manager: a FileHandler object to be used to call revert_certified_error_files and determine is_local

        Returns:
            A JsonResponse containing a success message

        Raises:
            ResponseException: if submission provided is a FABS submission or is not in an "updated" status
    """

    if submission.d2_submission:
        raise ResponseException('Submission must be a DABS submission.', status=StatusCode.CLIENT_ERROR)

    if submission.publish_status_id != PUBLISH_STATUS_DICT['updated']:
        raise ResponseException('Submission has not been certified or has not been updated since certification.',
                                status=StatusCode.CLIENT_ERROR)

    sess = GlobalDB.db().session
    move_certified_data(sess, submission.submission_id, direction='revert')

    # Copy file paths from certified_files_history
    max_cert_history = sess.query(func.max(CertifyHistory.certify_history_id), func.max(CertifyHistory.updated_at)).\
        filter(CertifyHistory.submission_id == submission.submission_id).one()
    remove_timestamp = [str(FILE_TYPE_DICT['appropriations']), str(FILE_TYPE_DICT['program_activity']),
                        str(FILE_TYPE_DICT['award_financial'])]
    if file_manager.is_local:
        filepath = CONFIG_BROKER['broker_files']
        ef_path = ''
    else:
        filepath = '{}/'.format(submission.submission_id)
        ef_path = filepath
        remove_timestamp.extend([str(FILE_TYPE_DICT['executive_compensation']), str(FILE_TYPE_DICT['sub_award'])])
    update_string = """
        WITH filenames AS (
            SELECT REVERSE(SPLIT_PART(REVERSE(filename), '/', 1)) AS simple_name,
                file_type_id
            FROM certified_files_history
            WHERE certify_history_id = {history_id}
        )
        UPDATE job
        SET filename = CASE WHEN job.file_type_id NOT IN (6, 7)
                THEN '{filepath}'
                ELSE '{ef_path}'
                END || simple_name,
            original_filename = CASE WHEN job.file_type_id NOT IN ({remove_timestamp})
                THEN simple_name
                ELSE substring(simple_name, position('_' in simple_name) + 1)
                END
        FROM filenames
        WHERE job.file_type_id = filenames.file_type_id
            AND job.submission_id = {submission_id};
    """.format(history_id=max_cert_history[0], filepath=filepath, ef_path=ef_path,
               remove_timestamp=', '.join(remove_timestamp), submission_id=submission.submission_id)
    sess.execute(update_string)

    # Set errors/warnings for the submission
    submission.number_of_errors = 0
    submission.number_of_warnings = sess.query(func.sum(CertifiedErrorMetadata.occurrences).label('total_warnings')).\
        join(Job, CertifiedErrorMetadata.job_id == Job.job_id).\
        filter(Job.submission_id == submission.submission_id).one().total_warnings

    # Set default numbers/status/last validation date for jobs then update warnings
    sess.query(Job).filter_by(submission_id=submission.submission_id).\
        update({'number_of_errors': 0, 'number_of_warnings': 0, 'job_status_id': JOB_STATUS_DICT['finished'],
                'last_validated': max_cert_history[1], 'error_message': None, 'file_generation_id': None})

    # Get list of jobs so we can update them
    job_list = sess.query(Job).\
        filter(Job.submission_id == submission.submission_id,
               Job.job_type_id.in_([JOB_TYPE_DICT['csv_record_validation'], JOB_TYPE_DICT['validation']]),
               Job.file_type_id.notin_([FILE_TYPE_DICT['sub_award'], FILE_TYPE_DICT['executive_compensation']])).all()

    # Fixing File table
    job_ids = [str(job.job_id) for job in job_list]
    update_string = """
            UPDATE file
            SET filename = job.filename,
                file_status_id = 1,
                headers_missing = NULL,
                headers_duplicated = NULL
            FROM job
            WHERE job.job_id = file.job_id
                AND job.job_id IN ({job_ids});
        """.format(job_ids=', '.join(job_ids))
    sess.execute(update_string)

    file_type_mapping = {
        FILE_TYPE_DICT['appropriations']: CertifiedAppropriation,
        FILE_TYPE_DICT['program_activity']: CertifiedObjectClassProgramActivity,
        FILE_TYPE_DICT['award_financial']: CertifiedAwardFinancial,
        FILE_TYPE_DICT['award']: CertifiedAwardFinancialAssistance,
        FILE_TYPE_DICT['award_procurement']: CertifiedAwardProcurement
    }
    # Update the number of warnings for each job in the list
    for job in job_list:
        job.number_of_warnings = sess.query(func.coalesce(func.sum(CertifiedErrorMetadata.occurrences), 0).
                                            label('total_warnings')). \
            filter_by(job_id=job.job_id).one().total_warnings
        # For non-cross-file jobs, also update the row count and file size
        if job.job_type_id != JOB_TYPE_DICT['validation']:
            file_type_model = file_type_mapping[job.file_type_id]
            total_rows = sess.query(file_type_model).filter_by(submission_id=submission.submission_id).count()
            job.number_of_rows = total_rows + 1
            job.number_of_rows_valid = total_rows
            if file_manager.is_local:
                # local file size
                try:
                    job.file_size = os.path.getsize(job.filename)
                except:
                    logger.warning("File doesn't exist locally: %s", job.filename)
                    job.file_size = 0
            else:
                # boto file size
                job.file_size = S3Handler.get_file_size(job.filename)
    # Set submission to certified status
    submission.publish_status_id = PUBLISH_STATUS_DICT['published']
    sess.commit()

    # Move warning files back non-locally and clear out error files for all environments
    file_manager.revert_certified_error_files(sess, max_cert_history[0])

    return JsonResponse.create(StatusCode.OK, {'message': 'Submission {} successfully reverted to certified status.'.
                               format(submission.submission_id)})
