from unittest.mock import Mock
import dataactbroker.handlers.fileHandler
import json
from tests.unit.dataactbroker.utils import add_models, delete_models
from tests.unit.dataactcore.factories.job import SubmissionFactory, JobFactory
from tests.unit.dataactcore.factories.user import UserFactory
from dataactbroker.routeUtils import InterfaceHolder
from dataactcore.models.jobModels import JobStatus, JobType, FileType

PAGE = 1
LIMIT = 10
CERTIFIED = "mixed"

def test_list_submissions_success(database, job_constants, monkeypatch):
    fh = dataactbroker.handlers.fileHandler.FileHandler(Mock(), InterfaceHolder())

    mock_value = Mock()
    mock_value.getName.return_value = 1
    monkeypatch.setattr(dataactbroker.handlers.fileHandler, 'LoginSession', mock_value)

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
    fh = dataactbroker.handlers.fileHandler.FileHandler(Mock(), InterfaceHolder())

    mock_value = Mock()
    mock_value.getName.return_value = 1
    monkeypatch.setattr(dataactbroker.handlers.fileHandler, 'LoginSession', mock_value)

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