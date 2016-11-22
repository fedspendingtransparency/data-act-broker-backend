import json
from unittest.mock import Mock

import pytest

from dataactbroker.handlers import fileHandler
from dataactcore.interfaces.interfaceHolder import InterfaceHolder
from dataactcore.models.jobModels import JobStatus, JobType, FileType
from dataactcore.utils.responseException import ResponseException
from tests.unit.dataactbroker.utils import add_models, delete_models
from tests.unit.dataactcore.factories.job import JobFactory, SubmissionFactory
from tests.unit.dataactcore.factories.user import UserFactory

PAGE = 1
LIMIT = 10
CERTIFIED = "mixed"

def test_list_submissions_success(database, job_constants, monkeypatch):
    fh = fileHandler.FileHandler(Mock(), InterfaceHolder())

    mock_value = Mock()
    mock_value.getName.return_value = 1
    monkeypatch.setattr(fileHandler, 'LoginSession', mock_value)

    user = UserFactory(user_id=1, cgac_code='cgac')
    sub = SubmissionFactory(user_id=1, submission_id=1, number_of_warnings=1, cgac_code='cgac')
    add_models(database, [user, sub])

    json_response = fh.list_submissions(PAGE, LIMIT, CERTIFIED)
    assert json.loads(json_response.get_data().decode("utf-8"))['total'] == 1
    assert json.loads(json_response.get_data().decode("utf-8"))['submissions'][0]['status'] == "validation_successful_warnings"
    delete_models(database, [user, sub])

    sess = database.session
    user = UserFactory(user_id=1, cgac_code='cgac')
    sub = SubmissionFactory(user_id=1, submission_id=1, cgac_code='cgac')
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    json_response = fh.list_submissions(PAGE, LIMIT, CERTIFIED)
    assert json.loads(json_response.get_data().decode("utf-8"))['total'] == 1
    assert json.loads(json_response.get_data().decode("utf-8"))['submissions'][0]['status'] == "validation_successful"
    delete_models(database, [user, sub, job])

    sess = database.session
    user = UserFactory(user_id=1, cgac_code='cgac')
    sub = SubmissionFactory(user_id=1, submission_id=1, cgac_code='cgac')
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='running').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    json_response = fh.list_submissions(PAGE, LIMIT, CERTIFIED)
    assert json.loads(json_response.get_data().decode("utf-8"))['total'] == 1
    assert json.loads(json_response.get_data().decode("utf-8"))['submissions'][0]['status'] == "running"
    delete_models(database, [user, sub, job])

    sess = database.session
    user = UserFactory(user_id=1, cgac_code='cgac')
    sub = SubmissionFactory(user_id=1, submission_id=1, cgac_code='cgac')
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='waiting').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    json_response = fh.list_submissions(PAGE, LIMIT, CERTIFIED)
    assert json.loads(json_response.get_data().decode("utf-8"))['total'] == 1
    assert json.loads(json_response.get_data().decode("utf-8"))['submissions'][0]['status'] == "waiting"
    delete_models(database, [user, sub, job])

    sess = database.session
    user = UserFactory(user_id=1, cgac_code='cgac')
    sub = SubmissionFactory(user_id=1, submission_id=1, cgac_code='cgac')
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='ready').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    json_response = fh.list_submissions(PAGE, LIMIT, CERTIFIED)
    assert json.loads(json_response.get_data().decode("utf-8"))['total'] == 1
    assert json.loads(json_response.get_data().decode("utf-8"))['submissions'][0]['status'] == "ready"
    delete_models(database, [user, sub, job])

def test_list_submissions_failure(database, job_constants, monkeypatch):
    fh = fileHandler.FileHandler(Mock(), InterfaceHolder())

    mock_value = Mock()
    mock_value.getName.return_value = 1
    monkeypatch.setattr(fileHandler, 'LoginSession', mock_value)

    user = UserFactory(user_id=1, cgac_code='cgac')
    sub = SubmissionFactory(user_id=1, submission_id=1, number_of_errors=1, cgac_code='cgac')
    add_models(database, [user, sub])

    json_response = fh.list_submissions(PAGE, LIMIT, CERTIFIED)
    assert json.loads(json_response.get_data().decode("utf-8"))['total'] == 1
    assert json.loads(json_response.get_data().decode("utf-8"))['submissions'][0]['status'] == "validation_errors"
    delete_models(database, [user, sub])

    sess = database.session
    user = UserFactory(user_id=1, cgac_code='cgac')
    sub = SubmissionFactory(user_id=1, submission_id=1, cgac_code='cgac')
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='failed').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    json_response = fh.list_submissions(PAGE, LIMIT, CERTIFIED)
    assert json.loads(json_response.get_data().decode("utf-8"))['total'] == 1
    assert json.loads(json_response.get_data().decode("utf-8"))['submissions'][0]['status'] == "failed"
    delete_models(database, [user, sub, job])

    sess = database.session
    user = UserFactory(user_id=1, cgac_code='cgac')
    sub = SubmissionFactory(user_id=1, submission_id=1, cgac_code='cgac')
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='invalid').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    json_response = fh.list_submissions(PAGE, LIMIT, CERTIFIED)
    assert json.loads(json_response.get_data().decode("utf-8"))['total'] == 1
    assert json.loads(json_response.get_data().decode("utf-8"))['submissions'][0]['status'] == "file_errors"
    delete_models(database, [user, sub, job])


def test_requires_submission_perm_no_submission(database, monkeypatch):
    """If no submission exists, we should see an exception"""
    monkeypatch.setattr(fileHandler, 'user_agency_matches',
                        Mock(return_value=True))

    sub = SubmissionFactory()
    database.session.add(sub)
    database.session.commit()

    fn = fileHandler.requires_submission_perms(Mock())
    # Does not raise exception
    fn(sub.submission_id)
    with pytest.raises(ResponseException):
        fn(sub.submission_id + 1)   # different submission id


def test_requires_submission_perm_check(database, monkeypatch):
    """If the user doesn't have a matching agency, we should see an
    exception"""
    sub = SubmissionFactory()
    database.session.add(sub)
    database.session.commit()

    fn = fileHandler.requires_submission_perms(Mock())
    # Does not raise exception
    monkeypatch.setattr(fileHandler, 'user_agency_matches',
                        Mock(return_value=True))
    fn(sub.submission_id)

    monkeypatch.setattr(fileHandler, 'user_agency_matches',
                        Mock(return_value=False))
    with pytest.raises(ResponseException):
        fn(sub.submission_id)


def test_narratives(database, job_constants, monkeypatch):
    """Verify that we can add, retrieve, and update submission narratives. Not
    quite a unit test as it covers a few functions in sequence"""
    monkeypatch.setattr(fileHandler, 'user_agency_matches',
                        Mock(return_value=True))
    sub1, sub2 = SubmissionFactory(), SubmissionFactory()
    database.session.add_all([sub1, sub2])
    database.session.commit()

    # Write some narratives
    result = fileHandler.update_narratives(
        sub1.submission_id, {'B': 'BBBBBB', 'E': 'EEEEEE'})
    assert result.status_code == 200
    result = fileHandler.update_narratives(
        sub2.submission_id, {'A': 'Submission2'})
    assert result.status_code == 200

    # Check the narratives
    result = fileHandler.narratives_for_submission(sub1.submission_id)
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
        sub1.submission_id, {'A': 'AAAAAA', 'E': 'E2E2E2'})
    assert result.status_code == 200

    # Verify the change worked
    result = fileHandler.narratives_for_submission(sub1.submission_id)
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
