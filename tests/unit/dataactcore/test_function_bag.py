import pytest

from unittest.mock import patch

from dataactcore.aws.sqsHandler import SQSMockQueue
from dataactcore.models.jobModels import JobDependency
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT
from dataactcore.interfaces.function_bag import check_job_dependencies

from tests.unit.dataactcore.factories.job import JobFactory, SubmissionFactory


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
