import calendar
from datetime import datetime

from dataactcore.interfaces.function_bag import get_utc_now
from dataactcore.models.jobModels import Submission, Job


def insert_submission(
    sess,
    submission_user_id,
    cgac_code=None,
    frec_code=None,
    start_date=None,
    end_date=None,
    is_quarter=False,
    number_of_errors=0,
    publish_status_id=1,
    is_fabs=False,
    updated_at=get_utc_now(),
    test_submission=False,
    published_submission_ids=[],
    certified=False,
    reporting_fiscal_year=None,
    reporting_fisacal_period=None,
):
    """Insert one submission into job tracker and get submission ID back."""
    publishable = True if number_of_errors == 0 else False
    if start_date is not None:
        start_date = datetime.strptime(start_date, "%m/%Y")
    if end_date is not None:
        end_date = datetime.strptime(end_date, "%m/%Y")
        end_date = datetime.strptime(
            str(end_date.year)
            + "/"
            + str(end_date.month)
            + "/"
            + str(calendar.monthrange(end_date.year, end_date.month)[1]),
            "%Y/%m/%d",
        ).date()
    sub = Submission(
        created_at=get_utc_now(),
        updated_at=updated_at,
        user_id=submission_user_id,
        cgac_code=cgac_code,
        frec_code=frec_code,
        reporting_start_date=start_date,
        reporting_end_date=end_date,
        reporting_fiscal_year=reporting_fiscal_year,
        reporting_fiscal_period=reporting_fisacal_period,
        is_quarter_format=is_quarter,
        number_of_errors=number_of_errors,
        publish_status_id=publish_status_id,
        publishable=publishable,
        is_fabs=is_fabs,
        test_submission=test_submission,
        published_submission_ids=published_submission_ids,
        certified=certified,
    )
    sess.add(sub)
    sess.commit()
    return sub.submission_id


def insert_job(
    sess,
    filetype,
    status,
    type_id,
    submission,
    job_id=None,
    filename=None,
    original_filename=None,
    file_size=None,
    num_rows=None,
    num_valid_rows=0,
    num_errors=0,
    updated_at=None,
):
    """Insert one job into job tracker and get ID back."""
    if not updated_at:
        updated_at = get_utc_now()

    job = Job(
        created_at=get_utc_now(),
        updated_at=updated_at,
        file_type_id=filetype,
        job_status_id=status,
        job_type_id=type_id,
        submission_id=submission,
        filename=filename,
        original_filename=original_filename,
        file_size=file_size,
        number_of_rows=num_rows,
        number_of_rows_valid=num_valid_rows,
        number_of_errors=num_errors,
    )
    if job_id:
        job.job_id = job_id
    sess.add(job)
    sess.commit()
    return job


def get_submission(sess, sub_id):
    """Get back the requested submission"""
    return sess.query(Submission).filter_by(submission_id=sub_id).one_or_none()
