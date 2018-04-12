import logging

from datetime import datetime
from sqlalchemy import func, or_

from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import sum_number_of_errors_for_job_list

from dataactcore.models.lookups import JOB_STATUS_DICT, PUBLISH_STATUS_DICT
from dataactcore.models.jobModels import FileRequest, Job, Submission, SubmissionSubTierAffiliation
from dataactcore.models.stagingModels import AwardFinancial

from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode

logger = logging.getLogger(__name__)


def create_submission(user_id, submission_values, existing_submission):
    """ Create a new submission

    Arguments:
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
    """Set number of errors and warnings for submission.

    Arguments:
        submission_id: submission to update submission error info

    Returns:
        submission object"""
    sess = GlobalDB.db().session
    submission = sess.query(Submission).filter(Submission.submission_id == submission_id).one()
    submission.number_of_errors = sum_number_of_errors_for_job_list(submission_id)
    submission.number_of_warnings = sum_number_of_errors_for_job_list(submission_id, error_type='warning')
    sess.commit()

    return submission


def get_submission_stats(submission_id):
    """Get summarized dollar amounts by submission.

    Arguments:
        submission_id: submission to retrieve info from

    Returns:
        object containing total_obligations, total_procurement_obligations, total_assistance_obligations"""
    sess = GlobalDB.db().session
    base_query = sess.query(func.sum(AwardFinancial.transaction_obligated_amou)).\
        filter(AwardFinancial.submission_id == submission_id)
    procurement = base_query.filter(AwardFinancial.piid.isnot(None))
    fin_assist = base_query.filter(or_(AwardFinancial.fain.isnot(None),
                                       AwardFinancial.uri.isnot(None)))
    return {
        "total_obligations": float(base_query.scalar() or 0),
        "total_procurement_obligations": float(procurement.scalar() or 0),
        "total_assistance_obligations": float(fin_assist.scalar() or 0)
    }


def get_submission_status(submission, jobs):
    """Return the status of a submission.

    Arguments:
        submission: submission to retrieve status from
        jobs: jobs within the submission to retrieve status from

    Returns:
        string containing the status of the submission"""
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
    """Return the filenames of all jobs within a submission.

    Arguments:
        jobs: jobs retrieve filenames from

    Returns:
        array of all filenames within the jobs given"""
    job_list = []
    for job in jobs:
        if job.filename not in job_list:
            job_list.append(job.filename)
    return job_list


def delete_all_submission_data(submission):
    """ Delete a submission, operates under the assumption

    Arguments:
        submission: submission to delete

    Returns:
        JsonResponse object
    """
    # check if the submission has been published, if so, do not allow deletion
    if submission.publish_status_id != PUBLISH_STATUS_DICT['unpublished']:
        return JsonResponse.error(ValueError("Submissions that have been certified cannot be deleted"),
                                  StatusCode.CLIENT_ERROR)

    sess = GlobalDB.db().session
    all_jobs = sess.query(Job).filter(Job.submission_id == submission.submission_id)

    # check if the submission has any jobs that are currently running, if so, do not allow deletion
    running_jobs = all_jobs.filter(Job.job_status_id == JOB_STATUS_DICT['running']).all()
    if running_jobs:
        return JsonResponse.error(ValueError("Submissions with running jobs cannot be deleted"),
                                  StatusCode.CLIENT_ERROR)

    logger.info({
        "message": "Deleting submission with id {}".format(submission.submission_id),
        "message_type": "BrokerInfo",
        "submission_id": submission.submission_id
    })

    for job in all_jobs.all():
        # check if the submission has any cached D files, if so, disconnect that job from the submission
        cached_file = sess.query(FileRequest).filter(FileRequest.job_id == job.job_id,
                                                     FileRequest.is_cached_file.is_(True)).all()
        if cached_file:
            job.submission_id = None
            sess.commit()

    sess.query(SubmissionSubTierAffiliation).\
        filter(SubmissionSubTierAffiliation.submission_id == submission.submission_id).\
        delete(synchronize_session=False)
    sess.query(Submission).filter(Submission.submission_id == submission.submission_id).\
        delete(synchronize_session=False)

    sess.expire_all()

    return JsonResponse.create(StatusCode.OK, {"message": "Success"})
