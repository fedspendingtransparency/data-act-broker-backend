from datetime import date, datetime
import json
from unittest.mock import Mock

import pytest

from dataactbroker.handlers import fileHandler
from dataactcore.models.jobModels import JobStatus, JobType, FileType
from dataactcore.utils.responseException import ResponseException
from tests.unit.dataactbroker.utils import add_models, delete_models
from tests.unit.dataactcore.factories.job import JobFactory, SubmissionFactory
from tests.unit.dataactcore.factories.user import UserFactory

PAGE = 1
LIMIT = 10
CERTIFIED = "mixed"

def test_list_submissions_success(database, job_constants, monkeypatch):
    fh = fileHandler.FileHandler(Mock())

    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, number_of_warnings=1)
    add_models(database, [user, sub])

    monkeypatch.setattr(fileHandler, 'g', Mock(user=user))
    json_response = fh.list_submissions(PAGE, LIMIT, CERTIFIED)
    assert json.loads(json_response.get_data().decode("utf-8"))['total'] == 1
    assert json.loads(json_response.get_data().decode("utf-8"))['submissions'][0]['status'] == "validation_successful_warnings"
    delete_models(database, [user, sub])

    sess = database.session
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1)
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    json_response = fh.list_submissions(PAGE, LIMIT, CERTIFIED)
    assert json.loads(json_response.get_data().decode("utf-8"))['total'] == 1
    assert json.loads(json_response.get_data().decode("utf-8"))['submissions'][0]['status'] == "validation_successful"
    delete_models(database, [user, sub, job])

    sess = database.session
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1)
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='running').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    json_response = fh.list_submissions(PAGE, LIMIT, CERTIFIED)
    assert json.loads(json_response.get_data().decode("utf-8"))['total'] == 1
    assert json.loads(json_response.get_data().decode("utf-8"))['submissions'][0]['status'] == "running"
    delete_models(database, [user, sub, job])

    sess = database.session
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1)
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='waiting').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    json_response = fh.list_submissions(PAGE, LIMIT, CERTIFIED)
    assert json.loads(json_response.get_data().decode("utf-8"))['total'] == 1
    assert json.loads(json_response.get_data().decode("utf-8"))['submissions'][0]['status'] == "waiting"
    delete_models(database, [user, sub, job])

    sess = database.session
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1)
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='ready').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    json_response = fh.list_submissions(PAGE, LIMIT, CERTIFIED)
    assert json.loads(json_response.get_data().decode("utf-8"))['total'] == 1
    assert json.loads(json_response.get_data().decode("utf-8"))['submissions'][0]['status'] == "ready"
    delete_models(database, [user, sub, job])

def test_list_submissions_failure(database, job_constants, monkeypatch):
    fh = fileHandler.FileHandler(Mock())

    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, number_of_errors=1)
    add_models(database, [user, sub])

    monkeypatch.setattr(fileHandler, 'g', Mock(user=user))
    json_response = fh.list_submissions(PAGE, LIMIT, CERTIFIED)
    assert json.loads(json_response.get_data().decode("utf-8"))['total'] == 1
    assert json.loads(json_response.get_data().decode("utf-8"))['submissions'][0]['status'] == "validation_errors"
    delete_models(database, [user, sub])

    sess = database.session
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1)
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='failed').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    json_response = fh.list_submissions(PAGE, LIMIT, CERTIFIED)
    assert json.loads(json_response.get_data().decode("utf-8"))['total'] == 1
    assert json.loads(json_response.get_data().decode("utf-8"))['submissions'][0]['status'] == "failed"
    delete_models(database, [user, sub, job])

    sess = database.session
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1)
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='invalid').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    json_response = fh.list_submissions(PAGE, LIMIT, CERTIFIED)
    assert json.loads(json_response.get_data().decode("utf-8"))['total'] == 1
    assert json.loads(json_response.get_data().decode("utf-8"))['submissions'][0]['status'] == "file_errors"
    delete_models(database, [user, sub, job])


def test_narratives(database, job_constants):
    """Verify that we can add, retrieve, and update submission narratives. Not
    quite a unit test as it covers a few functions in sequence"""
    sub1, sub2 = SubmissionFactory(), SubmissionFactory()
    database.session.add_all([sub1, sub2])
    database.session.commit()

    # Write some narratives
    result = fileHandler.update_narratives(
        sub1, {'B': 'BBBBBB', 'E': 'EEEEEE'})
    assert result.status_code == 200
    result = fileHandler.update_narratives(sub2, {'A': 'Submission2'})
    assert result.status_code == 200

    # Check the narratives
    result = fileHandler.narratives_for_submission(sub1)
    result = json.loads(result.get_data().decode('UTF-8'))
    assert result == {
        'A': '',
        'B': 'BBBBBB',
        'C': '',
        'D1': '',
        'D2': '',
        'E': 'EEEEEE',
        'F': ''
    }

    # Replace the narratives
    result = fileHandler.update_narratives(
        sub1, {'A': 'AAAAAA', 'E': 'E2E2E2'})
    assert result.status_code == 200

    # Verify the change worked
    result = fileHandler.narratives_for_submission(sub1)
    result = json.loads(result.get_data().decode('UTF-8'))
    assert result == {
        'A': 'AAAAAA',
        'B': '',
        'C': '',
        'D1': '',
        'D2': '',
        'E': 'E2E2E2',
        'F': ''
    }

good_dates = [
    ('04/2016', '05/2016', False, None),
    ('07/2014', '07/2014', False, None),
    ('01/2010', '03/2010', False, None),
    ('10/2017', '12/2017', True, None),
    ('04/2016', None, False, SubmissionFactory(
        reporting_start_date=datetime.strptime('09/2016', '%m/%Y').date(),
        reporting_end_date=datetime.strptime('09/2016', '%m/%Y').date())),
    (None, '07/2014', None, SubmissionFactory(
        reporting_start_date=datetime.strptime('08/2013', '%m/%Y').date(),
        reporting_end_date = datetime.strptime('09/2016', '%m/%Y').date())),
    ('01/2010', '03/2010', True, SubmissionFactory(is_quarter_format=False)),
    (None, None, None, SubmissionFactory(
        reporting_start_date=datetime.strptime('09/2016', '%m/%Y').date(),
        reporting_end_date=datetime.strptime('09/2016', '%m/%Y').date()
    )),
    (None, None, None, SubmissionFactory(
        is_quarter_format=True,
        reporting_start_date=datetime.strptime('10/2016', '%m/%Y').date(),
        reporting_end_date=datetime.strptime('12/2016', '%m/%Y').date()
    ))
]
@pytest.mark.parametrize("start_date, end_date, quarter_flag, submission", good_dates)
def test_submission_good_dates(start_date, end_date, quarter_flag, submission):
    fh = fileHandler.FileHandler(Mock())
    date_format = '%m/%Y'
    output_start_date, output_end_date = fh.check_submission_dates(start_date, end_date, quarter_flag, submission)
    assert isinstance(output_start_date, date)
    assert isinstance(output_end_date, date)
    # if we explicitly give a submission beginning or end date, those dates should
    # override the ones on the existing submission
    if start_date is None:
        assert output_start_date == submission.reporting_start_date
    else:
        assert output_start_date == datetime.strptime(start_date, date_format).date()
    if end_date is None:
        assert output_end_date == submission.reporting_end_date
    else:
        assert output_end_date == datetime.strptime(end_date, date_format).date()

bad_dates = [
    ('04/2016', '05/2016', True, SubmissionFactory()),
    ('08/2016', '11/2016', True, SubmissionFactory()),
    ('07/2014', '06/2014', False, None),
    ('11/2010', '03/2010', False, None),
    ('10/2017', '12/xyz', True, None),
    ('01/2016', '07/2016', True, None),
    (None, '01/1930', False, SubmissionFactory())
]
@pytest.mark.parametrize("start_date, end_date, quarter_flag, submission", bad_dates)
def test_submission_bad_dates(start_date, end_date, quarter_flag, submission):
    """Verify that submission date checks fail on bad input"""
    # all dates must be in mm/yyyy format
    # quarterly submissions:
    # - can span a single quarter only
    # - must end with month = 3, 6, 9, or 12
    fh = fileHandler.FileHandler(Mock())
    with pytest.raises(ResponseException):
        fh.check_submission_dates(start_date, end_date, quarter_flag, submission)
