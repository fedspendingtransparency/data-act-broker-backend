import pytest
import datetime
from unittest.mock import patch

from dataactcore.aws.sqsHandler import SQSMockQueue
from dataactcore.models.jobModels import JobDependency
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT
from dataactcore.interfaces.function_bag import check_job_dependencies, get_window_end, get_time_period

from tests.unit.dataactcore.factories.job import JobFactory, SubmissionFactory, QuarterlyRevalidationThresholdFactory


@pytest.mark.usefixtures("job_constants")
def test_check_job_dependencies_not_finished(database):
    """ Tests check_job_dependencies with a job that isn't finished """
    sess = database.session
    sub = SubmissionFactory(submission_id=1)
    job = JobFactory(submission_id=sub.submission_id, job_status_id=JOB_STATUS_DICT['waiting'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'])
    sess.add_all([sub, job])
    sess.commit()

    with pytest.raises(ValueError):
        check_job_dependencies(job.job_id)


@pytest.mark.usefixtures("job_constants")
def test_check_job_dependencies_has_unfinished_dependencies(database):
    """ Tests check_job_dependencies with a job that isn't finished """
    sess = database.session
    sub = SubmissionFactory(submission_id=1)
    job = JobFactory(submission_id=sub.submission_id, job_status_id=JOB_STATUS_DICT['finished'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'],
                     number_of_errors=0)
    job_2 = JobFactory(submission_id=sub.submission_id, job_status_id=JOB_STATUS_DICT['waiting'],
                       job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'])
    job_3 = JobFactory(submission_id=sub.submission_id, job_status_id=JOB_STATUS_DICT['waiting'],
                       job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'],
                       number_of_errors=0)
    sess.add_all([sub, job, job_2, job_3])
    sess.commit()

    # Job 1 finished, it is a prerequisite for job 2 (waiting)
    job_dep = JobDependency(job_id=job_2.job_id, prerequisite_id=job.job_id)
    # Job 3 is also a prerequisite of job 2, it's not done, job 2 should stay in "waiting"
    job_dep_2 = JobDependency(job_id=job_2.job_id, prerequisite_id=job_3.job_id)
    sess.add_all([job_dep, job_dep_2])
    sess.commit()

    check_job_dependencies(job.job_id)

    assert job_2.job_status_id == JOB_STATUS_DICT['waiting']


@pytest.mark.usefixtures("job_constants")
def test_check_job_dependencies_prior_dependency_has_errors(database):
    """ Tests check_job_dependencies with a job that is finished but has errors """
    sess = database.session
    sub = SubmissionFactory(submission_id=1)
    job = JobFactory(submission_id=sub.submission_id, job_status_id=JOB_STATUS_DICT['finished'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'],
                     number_of_errors=3)
    job_2 = JobFactory(submission_id=sub.submission_id, job_status_id=JOB_STATUS_DICT['waiting'],
                       job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'])
    sess.add_all([sub, job, job_2])
    sess.commit()

    # Job 1 finished, it is a prerequisite for job 2 (waiting) but it has errors
    job_dep = JobDependency(job_id=job_2.job_id, prerequisite_id=job.job_id)
    sess.add(job_dep)
    sess.commit()

    check_job_dependencies(job.job_id)

    assert job_2.job_status_id == JOB_STATUS_DICT['waiting']


@patch('dataactcore.interfaces.function_bag.sqs_queue')
@pytest.mark.usefixtures("job_constants")
def test_check_job_dependencies_ready(mock_sqs_queue, database):
    """ Tests check_job_dependencies with a job that can be set to ready """
    # Mock so it always returns the mock queue for the test
    mock_sqs_queue.return_value = SQSMockQueue
    sess = database.session
    sub = SubmissionFactory(submission_id=1)
    job = JobFactory(submission_id=sub.submission_id, job_status_id=JOB_STATUS_DICT['finished'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'],
                     number_of_errors=0)
    job_2 = JobFactory(submission_id=sub.submission_id, job_status_id=JOB_STATUS_DICT['waiting'],
                       job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'])
    sess.add_all([sub, job, job_2])
    sess.commit()

    # Job 1 finished, it is a prerequisite for job 2 (waiting) but it has errors
    job_dep = JobDependency(job_id=job_2.job_id, prerequisite_id=job.job_id)
    sess.add(job_dep)
    sess.commit()

    check_job_dependencies(job.job_id)

    assert job_2.job_status_id == JOB_STATUS_DICT['ready']


def test_get_window_end(database):
    """ Tests get_window_end with subs """
    sess = database.session
    quart_sub = SubmissionFactory(submission_id=1, reporting_fiscal_year=2020, reporting_fiscal_period=6,
                                  d2_submission=False, is_quarter_format=True)
    month_sub = SubmissionFactory(submission_id=2, reporting_fiscal_year=2020, reporting_fiscal_period=10,
                                  d2_submission=False, is_quarter_format=False)
    fail_sub = SubmissionFactory(submission_id=3, reporting_fiscal_year=2020, reporting_fiscal_period=9,
                                 d2_submission=False, is_quarter_format=False)
    d2_sub = SubmissionFactory(submission_id=4, reporting_fiscal_year=2020, reporting_fiscal_period=6,
                               d2_submission=True, is_quarter_format=False)
    q2 = QuarterlyRevalidationThresholdFactory(quarter=2, year=2020)
    q4 = QuarterlyRevalidationThresholdFactory(quarter=4, year=2020)
    sess.add_all([quart_sub, month_sub, fail_sub, d2_sub, q2, q4])

    # Pass cases
    assert get_window_end(quart_sub) == q2.window_end.date()
    assert get_window_end(month_sub) == q4.window_end.date()
    # Fail cases
    assert get_window_end(fail_sub) is None
    assert get_window_end(d2_sub) is None


def test_get_time_period(database):
    """ Tests get_time_period with subs """
    sess = database.session
    quart_sub = SubmissionFactory(submission_id=1, reporting_fiscal_year=2020, reporting_fiscal_period=6,
                                  d2_submission=False, is_quarter_format=True)
    month_sub = SubmissionFactory(submission_id=2, reporting_start_date=datetime.datetime(2020, 9, 10),
                                  d2_submission=False, is_quarter_format=False)
    sess.add_all([quart_sub, month_sub])

    # Pass cases
    assert get_time_period(quart_sub) == 'FY 20 / Q2'
    assert get_time_period(month_sub) == '09 / 2020'
