import logging
import os
import math

from datetime import datetime
from flask import g
from sqlalchemy import func, or_, desc, cast, Numeric
from sqlalchemy.sql.expression import case

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import (sum_number_of_errors_for_job_list, get_last_validated_date,
                                                 get_fabs_meta, get_error_type, get_error_metrics_by_job_id,
                                                 get_certification_deadline)

from dataactcore.models.lookups import (JOB_STATUS_DICT, PUBLISH_STATUS_DICT, JOB_TYPE_DICT, RULE_SEVERITY_DICT,
                                        FILE_TYPE_DICT)
from dataactcore.models.errorModels import ErrorMetadata, CertifiedErrorMetadata
from dataactcore.models.domainModels import CGAC, FREC
from dataactcore.models.jobModels import (Job, Submission, SubmissionSubTierAffiliation, Banner, CertifyHistory,
                                          PublishHistory, RevalidationThreshold, SubmissionWindowSchedule, Comment,
                                          CertifiedComment, PublishedFilesHistory)
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


def create_submission(user_id, submission_values, existing_submission, test_submission=False):
    """ Create a new submission if one doesn't exist, otherwise update the existing one

        Args:
            user_id:  user to associate with this submission
            submission_values: metadata about the submission
            existing_submission: id of existing submission (blank for new submissions)
            test_submission: a boolean flag to indicate whether the submission being created is a test or not, only
                used with new submissions

        Returns:
            submission object
    """
    if existing_submission is None:
        submission_values['test_submission'] = test_submission
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
        'total_obligations': float(base_query.scalar() or 0),
        'total_procurement_obligations': float(procurement.scalar() or 0),
        'total_assistance_obligations': float(fin_assist.scalar() or 0)
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

    certification_deadline = get_certification_deadline(submission)
    reporting_start = submission.reporting_start_date.strftime('%m/%d/%Y') if submission.reporting_start_date else None
    reporting_end = submission.reporting_end_date.strftime('%m/%d/%Y') if submission.reporting_end_date else None

    return {
        'cgac_code': submission.cgac_code,
        'frec_code': submission.frec_code,
        'agency_name': agency_name,
        'number_of_errors': submission.number_of_errors,
        'number_of_warnings': submission.number_of_warnings,
        'number_of_rows': number_of_rows,
        'total_size': total_size,
        'created_on': submission.created_at.strftime('%Y-%m-%dT%H:%M:%S'),
        'last_updated': submission.updated_at.strftime('%Y-%m-%dT%H:%M:%S'),
        'last_validated': last_validated,
        'reporting_period': reporting_date(submission),
        'reporting_start_date': reporting_start,
        'reporting_end_date': reporting_end,
        'publish_status': submission.publish_status.name,
        'quarterly_submission': submission.is_quarter_format,
        'test_submission': submission.test_submission,
        'published_submission_ids': submission.published_submission_ids,
        'certified': submission.certified,
        'certification_deadline': str(certification_deadline) if certification_deadline else '',
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
        'revalidation_threshold': reval_thresh.revalidation_date.strftime('%Y-%m-%dT%H:%M:%S') if reval_thresh else ''
    }


def get_latest_publication_period():
    """ Get the latest publication period for all submissions

        Returns:
            A dictionary containing the latest publication period (period and year)
    """
    sess = GlobalDB.db().session
    last_pub_period = sess.query(SubmissionWindowSchedule.period, SubmissionWindowSchedule.year).\
        filter(SubmissionWindowSchedule.period_start <= datetime.today()).\
        order_by(SubmissionWindowSchedule.period_start.desc()).first()
    return {
        'period': last_pub_period.period if last_pub_period else None,
        'year': last_pub_period.year if last_pub_period else None
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
    if submission.reporting_fiscal_period == 2:
        return 'P01-P02/{}'.format(str(submission.reporting_fiscal_year))
    return 'P{:02d}/{}'.format(submission.reporting_fiscal_period, submission.reporting_fiscal_year)


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
            'file_status': '',
            'error_type': '',
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
        job_info['duplicated_headers'] = StringCleaner.split_csv(file_results.headers_duplicated)
        job_info['error_type'] = get_error_type(job.job_id)
        job_info['error_data'] = get_error_metrics_by_job_id(job.job_id, job.job_type_name == 'validation',
                                                             severity_id=RULE_SEVERITY_DICT['fatal'])
        job_info['warning_data'] = get_error_metrics_by_job_id(job.job_id, job.job_type_name == 'validation',
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

    status = 'unknown'

    if statuses['failed'] != 0:
        status = 'failed'
    elif statuses['invalid'] != 0:
        status = 'file_errors'
    elif statuses['running'] != 0:
        status = 'running'
    elif statuses['waiting'] != 0:
        status = 'waiting'
    elif statuses['ready'] != 0:
        status = 'ready'
    elif statuses['finished'] == jobs.count():
        if submission.publish_status_id == PUBLISH_STATUS_DICT['unpublished']:
            status = 'validation_successful'
            if submission.number_of_warnings is not None and submission.number_of_warnings > 0:
                status = 'validation_successful_warnings'
        elif submission.publish_status_id == PUBLISH_STATUS_DICT['published']:
            status = 'certified' if submission.certified else 'published'
        elif submission.publish_status_id == PUBLISH_STATUS_DICT['updated']:
            status = 'updated'

    # Check if submission has errors
    if submission.number_of_errors is not None and submission.number_of_errors > 0:
        status = 'validation_errors'

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
        return JsonResponse.error(ValueError('Submissions that have been published cannot be deleted'),
                                  StatusCode.CLIENT_ERROR)

    sess = GlobalDB.db().session

    # check if the submission has any jobs that are currently running, if so, do not allow deletion
    running_jobs = sess.query(Job).filter(Job.submission_id == submission.submission_id,
                                          Job.job_status_id == JOB_STATUS_DICT['running']).all()
    if running_jobs:
        return JsonResponse.error(ValueError('Submissions with running jobs cannot be deleted'),
                                  StatusCode.CLIENT_ERROR)

    logger.info({
        'message': 'Deleting submission with id {}'.format(submission.submission_id),
        'message_type': 'BrokerInfo',
        'submission_id': submission.submission_id
    })

    sess.query(SubmissionSubTierAffiliation).\
        filter(SubmissionSubTierAffiliation.submission_id == submission.submission_id).\
        delete(synchronize_session=False)
    sess.query(Submission).filter(Submission.submission_id == submission.submission_id).\
        delete(synchronize_session=False)

    sess.expire_all()

    return JsonResponse.create(StatusCode.OK, {'message': 'Success'})


def list_banners():
    """ List all the banners that are open at the time this is called.

        Returns:
            A JsonResponse containing data for each currently open banner including start_date, end_date,
            notice_block, message, and type
    """
    current_banners = get_banners()

    data = []

    if current_banners.count() is 0:
        data = None
    else:
        for banner in current_banners:
            data.append({
                'start_date': str(banner.start_date),
                'end_date': str(banner.end_date),
                'notice_block': banner.block_certification,
                'header': banner.header,
                'message': banner.message,
                'type': banner.application_type.application_name,
                'banner_type': banner.banner_type
            })

    return JsonResponse.create(StatusCode.OK, {'data': data})


def get_banners():
    """ Get all banners that start before and end after the current date

        Returns:
            A query to get all the banners surrounding the current date
    """
    sess = GlobalDB.db().session

    curr_date = datetime.now().date()

    return sess.query(Banner).filter(Banner.start_date <= curr_date, Banner.end_date >= curr_date)


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
            'message': 'The current progress of this submission ID is on /v1/uploadDetachedFiles/ page.',
            'step': '6'
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
            'message': 'The current progress of this submission ID is on /v1/reviewData/ page.',
            'step': '5'
        }
        return JsonResponse.create(StatusCode.OK, data)

    # /v1/generateEF/
    if generate_ef.count() > 0:
        data = {
            'message': 'The current progress of this submission ID is on /v1/generateEF/ page.',
            'step': '4'
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
            'message': 'The current progress of this submission ID is on /v1/validateCrossFile/ page.',
            'step': '3'
        }
        return JsonResponse.create(StatusCode.OK, data)

    # /v1/generateFiles/
    if generate_files.count() == 3:
        data = {
            'message': 'The current progress of this submission ID is on /v1/generateFiles/ page.',
            'step': '2'
        }
        return JsonResponse.create(StatusCode.OK, data)

    # /v1/validateData/
    validate_data = sess.query(Job).filter(Job.submission_id == submission_id, Job.file_type_id.in_([1, 2, 3]),
                                           Job.job_type_id == 2, Job.number_of_errors != 0, Job.file_size.isnot(None))
    check_header_errors = sess.query(Job).filter(Job.submission_id == submission_id, Job.file_type_id.in_([1, 2, 3]),
                                                 Job.job_type_id == 2, Job.job_status_id != 4)
    if validate_data.count() or check_header_errors.count() > 0:
        data = {
            'message': 'The current progress of this submission ID is on /v1/validateData/ page.',
            'step': '1'
        }
        return JsonResponse.create(StatusCode.OK, data)

    else:
        return JsonResponse.error(ValueError('The submission ID returns no response'), StatusCode.CLIENT_ERROR)


def get_published_submission_ids(cgac_code, frec_code, reporting_fiscal_year, reporting_fiscal_period,
                                 is_quarter_format, submission_id=None):
    """ List any published submissions by the same agency in the same period

        Args:
            cgac_code: the CGAC code to check against or None if checking a FREC agency
            frec_code: the FREC code to check against or None if checking a CGAC agency
            reporting_fiscal_year: the year to check for
            reporting_fiscal_period: the period in the year to check for
            is_quarter_format: whether the submission being checked is a quarterly or monthly submission
            submission_id: the submission ID to check against (used when checking if this submission is being
                re-certified)

        Returns:
            A JsonResponse containing a list of the published submissions for that period
    """
    # We need either a cgac or a frec code for this function
    if not cgac_code and not frec_code:
        return JsonResponse.error(ValueError('CGAC or FR Entity Code required'), StatusCode.CLIENT_ERROR)

    pub_subs = get_submissions_in_period(cgac_code, frec_code, int(reporting_fiscal_year), int(reporting_fiscal_period),
                                         is_quarter_format, submission_id=submission_id, filter_published='published')
    published_submissions = [
        {
            'submission_id': pub_sub.submission_id,
            'is_quarter': pub_sub.is_quarter_format
        }
        for pub_sub in pub_subs
    ]
    return JsonResponse.create(StatusCode.OK, {'published_submissions': published_submissions})


def get_submissions_in_period(cgac_code, frec_code, reporting_fiscal_year, reporting_fiscal_period, is_quarter_format,
                              submission_id=None, filter_published='published'):
    """ Find all the submissions in the given period for the given CGAC or FREC code and submission type

        Args:
            cgac_code: the CGAC code to check against or None if checking a FREC agency
            frec_code: the FREC code to check against or None if checking a CGAC agency
            reporting_fiscal_year: the year to check for
            reporting_fiscal_period: the period in the year to check for
            is_quarter_format: whether the submission being checked is a quarterly or monthly submission
            submission_id: the submission ID to check against (used when checking if this submission is being
                re-certified)
            filter_published: whether to filter published/unpublished submissions
                       (options are: "mixed", "published" (default), and "unpublished")

        Returns:
            query including all the submissions for a given period
    """
    qtr_subs = filter_submissions(cgac_code, frec_code, reporting_fiscal_year, reporting_fiscal_period,
                                  submission_id, filter_quarter=True, filter_published=filter_published,
                                  filter_sub_type='quarterly')
    mon_subs = filter_submissions(cgac_code, frec_code, reporting_fiscal_year, reporting_fiscal_period,
                                  submission_id, filter_quarter=is_quarter_format,
                                  filter_published=filter_published,
                                  filter_sub_type='monthly')
    subs_in_period = mon_subs.union(qtr_subs)
    return subs_in_period


def filter_submissions(cgac_code, frec_code, reporting_fiscal_year, reporting_fiscal_period,
                       submission_id=None, filter_published='published', filter_quarter=False,
                       filter_sub_type='mixed'):
    """ Get the list of the submissions based on the filters provided

        Args:
            cgac_code: the CGAC code to check against or None if checking a FREC agency
            frec_code: the FREC code to check against or None if checking a CGAC agency
            reporting_fiscal_year: the year to check for
            reporting_fiscal_period: the period in the year to check for
            submission_id: the submission ID to check against (used when checking if this submission is being
                re-certified)
            filter_published: whether to filter published/unpublished submissions
                       (options are: "mixed", "published" (default), and "unpublished")
            filter_quarter: whether to include submissions in the same quarter (True) or period (False, default)
            filter_sub_type: whether to include monthly and/or quarterly submissions
                             (options are: "monthly", "quarterly", and "mixed" (default))

        Returns:
            A query to get submissions based on the filters provided
    """
    sess = GlobalDB.db().session

    submission_query = sess.query(Submission).filter(
        (Submission.cgac_code == cgac_code) if cgac_code else (Submission.frec_code == frec_code),
        Submission.reporting_fiscal_year == reporting_fiscal_year,
        Submission.d2_submission.is_(False))

    if filter_published not in ('published', 'unpublished', 'mixed'):
        raise ValueError('Published param must be one of the following: "published", "unpublished", or "mixed"')
    if filter_published == 'published':
        submission_query = submission_query.filter(Submission.publish_status_id != PUBLISH_STATUS_DICT['unpublished'])
    elif filter_published == 'unpublished':
        submission_query = submission_query.filter(Submission.publish_status_id == PUBLISH_STATUS_DICT['unpublished'])

    if not filter_quarter:
        submission_query = submission_query.filter(Submission.reporting_fiscal_period == reporting_fiscal_period)
    else:
        reporting_fiscal_quarter = math.ceil(reporting_fiscal_period / 3)
        submission_query = submission_query.filter((func.ceil(cast(Submission.reporting_fiscal_period, Numeric) / 3) ==
                                                    reporting_fiscal_quarter))

    if filter_sub_type not in ('monthly', 'quarterly', 'mixed'):
        raise ValueError('Published param must be one of the following: "monthly", "quarterly", or "mixed"')
    if filter_sub_type == 'monthly':
        submission_query = submission_query.filter(Submission.is_quarter_format.is_(False))
    elif filter_sub_type == 'quarterly':
        submission_query = submission_query.filter(Submission.is_quarter_format.is_(True))

    # Filter out the submission we are potentially re-certifying if one is provided
    if submission_id:
        submission_query = submission_query.filter(Submission.submission_id != submission_id)

    return submission_query.order_by(desc(Submission.created_at))


def move_published_data(sess, submission_id, direction='publish'):
    """ Move data from the staging tables to the certified tables for a submission or do the reverse for a revert.

        Args:
            sess: the database connection
            submission_id: The ID of the submission to move data for
            direction: The direction to move the published data (publish or revert)

        Raises:
            ResponseException if a value other than "publish" or "revert" is specified for the direction.
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
        if direction == 'publish':
            source_table = table_object[0]
            target_table = table_object[1]
        elif direction == 'revert':
            source_table = table_object[1]
            target_table = table_object[0]
        else:
            raise ResponseException('Direction to move data must be publish or revert.', status=StatusCode.CLIENT_ERROR)

        logger.info({
            'message': 'Deleting old data from {} table'.format(target_table.__table__.name),
            'message_type': 'BrokerInfo',
            'submission_id': submission_id
        })

        # Delete the old data in the target table
        if table_object[2] == 'submission':
            sess.query(target_table).filter_by(submission_id=submission_id).delete(synchronize_session=False)
        else:
            sess.query(target_table).filter(target_table.job_id.in_(job_list)).delete(synchronize_session=False)

        logger.info({
            'message': 'Moving published data from {} table'.format(source_table.__table__.name),
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

        # Move the published data
        sess.execute(insert_string)


def publish_checks(submission):
    """ Checks to make sure the submission can be published

        Args:
            submission: the submission to be published

        Raises:
            ValueError if there is any reason a submission cannot be published
    """

    if not submission.publishable:
        raise ValueError('Submission cannot be published due to critical errors')

    if submission.test_submission:
        raise ValueError('Test submissions cannot be published')

    if submission.publish_status_id == PUBLISH_STATUS_DICT['published']:
        raise ValueError('Submission has already been published')

    if submission.publish_status_id in (PUBLISH_STATUS_DICT['publishing'], PUBLISH_STATUS_DICT['reverting']):
        raise ValueError('Submission is publishing or reverting')

    banners = get_banners()
    for banner in banners:
        if banner.block_certification:
            raise ValueError(banner.message)

    sess = GlobalDB.db().session
    # Check revalidation threshold
    last_validated = get_last_validated_date(submission.submission_id)
    reval_thresh = get_revalidation_threshold()['revalidation_threshold']
    if reval_thresh and reval_thresh >= last_validated:
        raise ValueError('This submission has not been validated since before the revalidation threshold ({}), it must'
                         ' be revalidated before publishing.'.format(reval_thresh.replace('T', ' ')))

    # Get the year/period of the submission and filter by them
    sub_period = submission.reporting_fiscal_period
    sub_year = submission.reporting_fiscal_year
    sub_schedule = sess.query(SubmissionWindowSchedule).filter_by(year=sub_year, period=sub_period). \
        one_or_none()
    # If we don't have a submission window for this year/period, they can't submit
    if not sub_schedule:
        raise ValueError('No submission window for this year and period was found. If this is an error, please contact'
                         ' the Service Desk.')

    # Make sure everything was last validated after the start of the submission window
    last_validated = datetime.strptime(last_validated, '%Y-%m-%dT%H:%M:%S')
    if last_validated < sub_schedule.period_start:
        raise ValueError('This submission was last validated or its D files generated before the start of the'
                         ' submission window ({}). Please revalidate before publishing.'.
                         format(sub_schedule.period_start.strftime('%m/%d/%Y')))

    # Make sure neither A nor B is blank before allowing certification
    blank_files = sess.query(Job). \
        filter(Job.file_type_id.in_([FILE_TYPE_DICT['appropriations'], FILE_TYPE_DICT['program_activity']]),
               Job.number_of_rows_valid == 0, Job.job_type_id == JOB_TYPE_DICT['csv_record_validation'],
               Job.submission_id == submission.submission_id).count()

    if blank_files > 0:
        raise ValueError('Cannot publish while file A or B is blank.')

    pub_subs = get_submissions_in_period(submission.cgac_code, submission.frec_code, submission.reporting_fiscal_year,
                                         submission.reporting_fiscal_period, submission.is_quarter_format,
                                         submission_id=submission.submission_id, filter_published='published')

    if pub_subs.count() > 0:
        raise ValueError('This period already has published submission(s) by this agency.')


def process_dabs_publish(submission, file_manager):
    """ Processes the actual publishing of a DABS submission.

        Args:
            submission: the submission to be published
            file_manager: a FileHandler object to be used to call move_published_files
    """
    publish_checks(submission)

    current_user_id = g.user.user_id
    sess = GlobalDB.db().session

    # Determine if this is the first time this submission is being published
    first_publish = (submission.publish_status_id == PUBLISH_STATUS_DICT['unpublished'])

    # set publish_status to "publishing"
    submission.publish_status_id = PUBLISH_STATUS_DICT['publishing']

    # create the publish_history entry
    publish_history = PublishHistory(created_at=datetime.utcnow(), user_id=current_user_id,
                                     submission_id=submission.submission_id)
    sess.add(publish_history)
    sess.commit()

    # get the publish_history entry including the PK
    publish_history = sess.query(PublishHistory).filter_by(submission_id=submission.submission_id). \
        order_by(PublishHistory.created_at.desc()).first()

    # Move the data to the certified table, deleting any old published data in the process
    move_published_data(sess, submission.submission_id)

    # move files (locally we don't move but we still need to populate the published_files_history table)
    file_manager.move_published_files(submission, publish_history, None, file_manager.is_local)

    # set submission contents
    submission.publishing_user_id = current_user_id
    submission.publish_status_id = PUBLISH_STATUS_DICT['published']
    sess.commit()

    if first_publish:
        # update any other submissions by the same agency in the same quarter/period to point to this submission
        unpub_subs = get_submissions_in_period(submission.cgac_code, submission.frec_code,
                                               submission.reporting_fiscal_year, submission.reporting_fiscal_period,
                                               submission.is_quarter_format, submission.submission_id,
                                               filter_published='unpublished')
        for unpub_sub in unpub_subs.all():
            unpub_sub.published_submission_ids.append(submission.submission_id)
            unpub_sub.test_submission = True
        sess.commit()


def process_dabs_certify(submission):
    """ Processes the actual certification of a DABS submission.

        Args:
            submission: the submission to be certified

        Raises:
            ValueError if this is somehow called without a PublishHistory associated with the submission ID or if there
            is already a certification associated with the most recent publication
    """
    current_user_id = g.user.user_id
    sess = GlobalDB.db().session

    max_pub_history = sess.query(func.max(PublishHistory.publish_history_id).label('max_id')). \
        filter(PublishHistory.submission_id == submission.submission_id).one()

    if max_pub_history.max_id is None:
        raise ValueError('There is no publish history associated with this submission. Submission must be published'
                         ' before certification.')

    pub_files_history = sess.query(PublishedFilesHistory).filter_by(publish_history_id=max_pub_history.max_id).all()

    for pub_file in pub_files_history:
        if pub_file.certify_history_id is not None:
            raise ValueError('This submission already has a certification associated with the most recent publication.')

    certify_history = CertifyHistory(created_at=datetime.utcnow(), user_id=current_user_id,
                                     submission_id=submission.submission_id)
    sess.add(certify_history)
    sess.commit()

    for pub_file in pub_files_history:
        pub_file.certify_history_id = certify_history.certify_history_id

    submission.certified = True
    sess.commit()


def publish_dabs_submission(submission, file_manager):
    """ Publish a DABS submission (monthly only)

        Args:
            submission: the submission to be published
            file_manager: a FileHandler object to be used to call move_published_files

        Returns:
            A JsonResponse containing the message "success" if successful, JsonResponse error containing the details of
            the error if something went wrong
    """
    if submission.is_quarter_format:
        return JsonResponse.error(ValueError('Quarterly submissions cannot be published separate from certification.'
                                             ' Use the publish_and_certify_dabs_submission endpoint to publish and'
                                             ' certify.'),
                                  StatusCode.CLIENT_ERROR)
    if submission.certified:
        return JsonResponse.error(ValueError('Submissions that have been certified cannot be republished separately.'
                                             ' Use the publish_and_certify_dabs_submission endpoint to republish.'),
                                  StatusCode.CLIENT_ERROR)

    # Get the year/period of the submission and filter by them
    sess = GlobalDB.db().session
    sub_period = submission.reporting_fiscal_period
    sub_year = submission.reporting_fiscal_year
    sub_schedule = sess.query(SubmissionWindowSchedule).filter_by(year=sub_year, period=sub_period).one_or_none()

    # If this is a monthly submission and the certification deadline has passed they have to publish/certify together
    if sub_schedule and sub_schedule.certification_deadline < datetime.utcnow():
        return JsonResponse.error(ValueError('Monthly submissions past their certification deadline must be published'
                                             ' and certified at the same time. Use the'
                                             ' publish_and_certify_dabs_submission endpoint.'), StatusCode.CLIENT_ERROR)

    try:
        process_dabs_publish(submission, file_manager)
    except ValueError as e:
        return JsonResponse.error(e, StatusCode.CLIENT_ERROR)

    return JsonResponse.create(StatusCode.OK, {'message': 'Success'})


def certify_dabs_submission(submission):
    """ Certify a DABS submission (monthly only)

        Args:
            submission: the submission to be certified

        Returns:
            A JsonResponse containing the message "success" if successful, JsonResponse error containing the details of
            the error if something went wrong
    """
    if submission.is_quarter_format:
        return JsonResponse.error(ValueError('Quarterly submissions cannot be certified separate from publication.'
                                             ' Use the publish_and_certify_dabs_submission endpoint to publish and'
                                             ' certify.'),
                                  StatusCode.CLIENT_ERROR)
    if submission.publish_status_id != PUBLISH_STATUS_DICT['published']:
        return JsonResponse.error(ValueError('Submissions must be published before certification. Use the'
                                             ' publish_dabs_submission endpoint to publish first.'),
                                  StatusCode.CLIENT_ERROR)
    if submission.certified:
        return JsonResponse.error(ValueError('Submissions that have been certified cannot be recertified separately.'
                                             ' Use the publish_and_certify_dabs_submission endpoint to recertify.'),
                                  StatusCode.CLIENT_ERROR)

    try:
        process_dabs_certify(submission)
    except ValueError as e:
        return JsonResponse.error(e, StatusCode.CLIENT_ERROR)

    return JsonResponse.create(StatusCode.OK, {'message': 'Success'})


def publish_and_certify_dabs_submission(submission, file_manager):
    """ Publish and certify a DABS submission

        Args:
            submission: the submission to be published and certified
            file_manager: a FileHandler object to be used to call move_published_files

        Returns:
            A JsonResponse containing the message "success" if successful, JsonResponse error containing the details of
            the error if something went wrong
    """
    try:
        process_dabs_publish(submission, file_manager)
    except ValueError as e:
        return JsonResponse.error(e, StatusCode.CLIENT_ERROR)

    try:
        process_dabs_certify(submission)
    except ValueError as e:
        return JsonResponse.error(e, StatusCode.CLIENT_ERROR)

    return JsonResponse.create(StatusCode.OK, {'message': 'Success'})


def revert_to_certified(submission, file_manager):
    """ Revert an updated DABS submission to its last published state

        Args:
            submission: the submission to be reverted
            file_manager: a FileHandler object to be used to call revert_published_error_files and determine is_local

        Returns:
            A JsonResponse containing a success message

        Raises:
            ResponseException: if submission provided is a FABS submission or is not in an "updated" status
    """

    if submission.d2_submission:
        raise ResponseException('Submission must be a DABS submission.', status=StatusCode.CLIENT_ERROR)

    if submission.publish_status_id != PUBLISH_STATUS_DICT['updated']:
        raise ResponseException('Submission has not been published or has not been updated since publication.',
                                status=StatusCode.CLIENT_ERROR)

    sess = GlobalDB.db().session
    submission.publish_status_id = PUBLISH_STATUS_DICT['reverting']
    sess.commit()
    move_published_data(sess, submission.submission_id, direction='revert')

    # Copy file paths from published_files_history
    max_pub_history = sess.query(func.max(PublishHistory.publish_history_id), func.max(PublishHistory.updated_at)).\
        filter(PublishHistory.submission_id == submission.submission_id).one()
    remove_timestamp = [str(FILE_TYPE_DICT['appropriations']), str(FILE_TYPE_DICT['program_activity']),
                        str(FILE_TYPE_DICT['award_financial'])]
    if file_manager.is_local:
        filepath = CONFIG_BROKER['broker_files']
        ef_path = ''
    else:
        filepath = '{}/'.format(submission.submission_id)
        ef_path = filepath
        remove_timestamp.extend([str(FILE_TYPE_DICT['executive_compensation']), str(FILE_TYPE_DICT['sub_award'])])

    # Published filename -> Job filename, original filename
    # Local:
    #   A/B/C:
    #     filename -> '[broker_files dir][published file base name]'
    #     original_filename -> '[published file base name without the timestamp]'
    #   D1/D2:
    #     filename -> '[broker_files dir][published file base name]'
    #     original_filename -> '[published file base name]'
    #   E/F:
    #     filename -> '[published file base name]'
    #     original_filename -> '[published file base name]'
    # Remote:
    #   A/B/C/E/F:
    #     filename -> '[submission_id]/[published file base name]'
    #     original_filename -> '[published file base name without the timestamp]'
    #   D1/D2:
    #     filename -> '[submission_id dir][published file base name]'
    #     original_filename -> '[published file base name]'
    update_string = """
        WITH filenames AS (
            SELECT REVERSE(SPLIT_PART(REVERSE(filename), '/', 1)) AS simple_name,
                file_type_id
            FROM published_files_history
            WHERE publish_history_id = {history_id}
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
    """.format(history_id=max_pub_history[0], filepath=filepath, ef_path=ef_path,
               remove_timestamp=', '.join(remove_timestamp), submission_id=submission.submission_id)
    sess.execute(update_string)

    # Set errors/warnings for the submission
    submission.number_of_errors = 0
    submission.number_of_warnings =\
        sess.query(func.coalesce(func.sum(CertifiedErrorMetadata.occurrences), 0).label('total_warnings')).\
        join(Job, CertifiedErrorMetadata.job_id == Job.job_id).\
        filter(Job.submission_id == submission.submission_id).one().total_warnings
    submission.publishable = True

    # Set default numbers/status/last validation date for jobs then update warnings
    sess.query(Job).filter_by(submission_id=submission.submission_id).\
        update({'number_of_errors': 0, 'number_of_warnings': 0, 'job_status_id': JOB_STATUS_DICT['finished'],
                'last_validated': max_pub_history[1], 'error_message': None, 'file_generation_id': None})

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
                    logger.warning('File doesn\'t exist locally: %s', job.filename)
                    job.file_size = 0
            else:
                # boto file size
                job.file_size = S3Handler.get_file_size(job.filename)
    # Set submission to published status
    submission.publish_status_id = PUBLISH_STATUS_DICT['published']
    sess.commit()

    # Move warning files back non-locally and clear out error files for all environments
    file_manager.revert_published_error_files(sess, max_pub_history[0])

    return JsonResponse.create(StatusCode.OK, {'message': 'Submission {} successfully reverted to published status.'.
                               format(submission.submission_id)})
