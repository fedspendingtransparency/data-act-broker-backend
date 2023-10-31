import pytest
import datetime
from unittest.mock import patch

from dataactcore.aws.sqsHandler import SQSMockQueue
from dataactcore.models.jobModels import JobDependency
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT
from dataactcore.interfaces.function_bag import (check_job_dependencies, get_certification_deadline, get_time_period,
                                                 get_last_modified, filename_fyp_format)

from tests.unit.dataactcore.factories.job import JobFactory, SubmissionFactory, SubmissionWindowScheduleFactory


@pytest.mark.usefixtures("job_constants")
def test_check_job_dependencies_not_finished(database):
    """ Tests check_job_dependencies with a job that isn't finished """
    sess = database.session
    sub = SubmissionFactory(submission_id=1)
    job = JobFactory(submission=sub, job_status_id=JOB_STATUS_DICT['waiting'],
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
    job = JobFactory(submission=sub, job_status_id=JOB_STATUS_DICT['finished'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'],
                     number_of_errors=0)
    job_2 = JobFactory(submission=sub, job_status_id=JOB_STATUS_DICT['waiting'],
                       job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'])
    job_3 = JobFactory(submission=sub, job_status_id=JOB_STATUS_DICT['waiting'],
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
    job = JobFactory(submission=sub, job_status_id=JOB_STATUS_DICT['finished'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'],
                     number_of_errors=3)
    job_2 = JobFactory(submission=sub, job_status_id=JOB_STATUS_DICT['waiting'],
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
    job = JobFactory(submission=sub, job_status_id=JOB_STATUS_DICT['finished'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'],
                     number_of_errors=0)
    job_2 = JobFactory(submission=sub, job_status_id=JOB_STATUS_DICT['waiting'],
                       job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'])
    sess.add_all([sub, job, job_2])
    sess.commit()

    # Job 1 finished, it is a prerequisite for job 2 (waiting) but it has errors
    job_dep = JobDependency(job_id=job_2.job_id, prerequisite_id=job.job_id)
    sess.add(job_dep)
    sess.commit()

    check_job_dependencies(job.job_id)

    assert job_2.job_status_id == JOB_STATUS_DICT['ready']


def test_get_certification_deadline(database):
    """ Tests get_certification_deadline with subs """
    sess = database.session
    quart_sub = SubmissionFactory(submission_id=1, reporting_fiscal_year=2020, reporting_fiscal_period=6,
                                  is_fabs=False, is_quarter_format=True)
    month_sub = SubmissionFactory(submission_id=2, reporting_fiscal_year=2020, reporting_fiscal_period=10,
                                  is_fabs=False, is_quarter_format=False)
    fail_sub = SubmissionFactory(submission_id=3, reporting_fiscal_year=2020, reporting_fiscal_period=9,
                                 is_fabs=False, is_quarter_format=False)
    fabs_sub = SubmissionFactory(submission_id=4, reporting_fiscal_year=2020, reporting_fiscal_period=6,
                                 is_fabs=True, is_quarter_format=False)
    q2 = SubmissionWindowScheduleFactory(period=6, year=2020)
    p10 = SubmissionWindowScheduleFactory(period=10, year=2020)
    sess.add_all([quart_sub, month_sub, fail_sub, fabs_sub, q2, p10])

    # Pass cases
    assert get_certification_deadline(quart_sub) == q2.certification_deadline.date()
    assert get_certification_deadline(month_sub) == p10.certification_deadline.date()
    # Fail cases
    assert get_certification_deadline(fail_sub) is None
    assert get_certification_deadline(fabs_sub) is None


def test_get_time_period(database):
    """ Tests get_time_period with subs """
    sess = database.session
    quart_sub = SubmissionFactory(submission_id=1, reporting_fiscal_year=2020, reporting_fiscal_period=6,
                                  is_fabs=False, is_quarter_format=True)
    month_sub = SubmissionFactory(submission_id=2, reporting_start_date=datetime.datetime(2020, 9, 10),
                                  is_fabs=False, is_quarter_format=False)
    sess.add_all([quart_sub, month_sub])

    # Pass cases
    assert get_time_period(quart_sub) == 'FY 20 / Q2'
    assert get_time_period(month_sub) == '09 / 2020'


def test_filename_fyp_format():
    """ Tests filename_fyp_format """
    assert filename_fyp_format(2022, 3, True) == 'FY22Q1'
    assert filename_fyp_format('2022', 12, True) == 'FY22Q4'
    assert filename_fyp_format(2022, '4', False) == 'FY22P04'
    assert filename_fyp_format(2022, 11, False) == 'FY22P11'
    assert filename_fyp_format(2022, 2, False) == 'FY22P01-P02'


@pytest.mark.usefixtures("job_constants")
def test_get_last_modified(database):
    """ Tests get_last_modified """
    now = datetime.datetime.now()
    sess = database.session
    sub_1 = SubmissionFactory(submission_id=1, reporting_fiscal_year=2020, reporting_fiscal_period=6,
                              is_fabs=False, is_quarter_format=True, updated_at=now)
    sub_2 = SubmissionFactory(submission_id=2, reporting_fiscal_year=2020, reporting_fiscal_period=6,
                              is_fabs=False, is_quarter_format=True, updated_at=now)
    job = JobFactory(submission=sub_2, job_status_id=JOB_STATUS_DICT['waiting'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'],
                     updated_at=now + datetime.timedelta(days=1))
    sess.add_all([sub_1, sub_2, job])

    assert get_last_modified(sub_1.submission_id) == now
    assert get_last_modified(sub_2.submission_id) == now + datetime.timedelta(days=1)
    assert get_last_modified(0) is None
