from unittest.mock import Mock
import dataactbroker.handlers.accountHandler
import json
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from tests.unit.dataactbroker.utils import add_models, delete_models
from tests.unit.dataactcore.factories.job import SubmissionFactory, JobFactory
from tests.unit.dataactcore.factories.user import UserFactory
from dataactbroker.routeUtils import InterfaceHolder
from dataactcore.models.jobModels import JobStatus, JobType, FileType

def test_max_login_success(database, user_constants, monkeypatch):
    ah = dataactbroker.handlers.accountHandler.AccountHandler(Mock())

    mock_dict = Mock()
    mock_dict.return_value.safeDictionary.side_effect = {'ticket': '', 'service': ''}
    monkeypatch.setattr(dataactbroker.handlers.accountHandler, 'RequestDictionary', mock_dict)

    max_dict= {'cas:serviceResponse': {}}
    monkeypatch.setattr(ah, 'get_max_dict', Mock(return_value=max_dict))
    config = {'parent_group': 'parent-group'}
    monkeypatch.setattr(dataactbroker.handlers.accountHandler, 'CONFIG_BROKER', config)
    max_dict = {
        'cas:serviceResponse':
            {
                'cas:authenticationSuccess':
                    {
                        'cas:attributes':
                            {
                                'maxAttribute:Email-Address': 'test-user@email.com',
                                'maxAttribute:GroupList': 'parent-group,parent-group-CGAC_SYS',
                                'maxAttribute:First-Name': 'test',
                                'maxAttribute:Middle-Name': '',
                                'maxAttribute:Last-Name': 'user'
                            }
                    }
            }
    }
    monkeypatch.setattr(ah, 'get_max_dict', Mock(return_value=max_dict))

    # If it gets to this point, that means the user was in all the right groups aka successful login
    monkeypatch.setattr(ah, 'create_session_and_response',
                        Mock(return_value=JsonResponse.create(StatusCode.OK, {"message": "Login successful"})))
    json_response = ah.max_login(Mock())

    assert "Login successful" == json.loads(json_response.get_data().decode("utf-8"))['message']


def test_max_login_failure(monkeypatch):
    ah = dataactbroker.handlers.accountHandler.AccountHandler(Mock())
    config = {'parent_group': 'parent-group'}
    monkeypatch.setattr(dataactbroker.handlers.accountHandler, 'CONFIG_BROKER', config)

    mock_dict = Mock()
    mock_dict.return_value.safeDictionary.side_effect = {'ticket': '', 'service': ''}
    monkeypatch.setattr(dataactbroker.handlers.accountHandler, 'RequestDictionary', mock_dict)

    max_dict= {'cas:serviceResponse': {}}
    monkeypatch.setattr(ah, 'get_max_dict', Mock(return_value=max_dict))
    json_response = ah.max_login(Mock())
    error_message = "You have failed to login successfully with MAX"

    # Did not get a successful response from MAX
    assert error_message == json.loads(json_response.get_data().decode("utf-8"))['message']

    max_dict = {
                    'cas:serviceResponse':
                    {
                        'cas:authenticationSuccess':
                        {
                            'cas:attributes':
                            {
                                'maxAttribute:Email-Address': '',
                                'maxAttribute:GroupList': ''
                            }
                        }
                     }
                }
    monkeypatch.setattr(ah, 'get_max_dict', Mock(return_value=max_dict))
    json_response = ah.max_login(Mock())
    error_message = "You have logged in with MAX but do not have permission to access the broker. " \
                    "Please contact DATABroker@fiscal.treasury.gov to obtain access."

    # Not in parent group
    assert error_message == json.loads(json_response.get_data().decode("utf-8"))['message']

    max_dict = {
        'cas:serviceResponse':
            {
                'cas:authenticationSuccess':
                    {
                        'cas:attributes':
                            {
                                'maxAttribute:Email-Address': '',
                                'maxAttribute:GroupList': 'parent-group'
                            }
                    }
            }
    }
    monkeypatch.setattr(ah, 'get_max_dict', Mock(return_value=max_dict))
    json_response = ah.max_login(Mock())
    error_message = "You have logged in with MAX but do not have permission to access the broker. " \
                    "Please contact DATABroker@fiscal.treasury.gov to obtain access."

    # Not in cgac group
    assert error_message == json.loads(json_response.get_data().decode("utf-8"))['message']


def test_list_submissions_success(database, job_constants, monkeypatch):
    ah = dataactbroker.handlers.accountHandler.AccountHandler(Mock(), InterfaceHolder())

    mock_value = Mock()
    mock_value.getName.return_value = 1
    monkeypatch.setattr(dataactbroker.handlers.accountHandler, 'LoginSession', mock_value)

    user = UserFactory(user_id=1, cgac_code='cgac')
    sub = SubmissionFactory(user_id=1, submission_id=1, number_of_warnings=1, cgac_code='cgac')
    add_models(database, [user, sub])

    json_response = ah.list_submissions(None, None, None)
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

    json_response = ah.list_submissions(None, None, None)
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

    json_response = ah.list_submissions(None, None, None)
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

    json_response = ah.list_submissions(None, None, None)
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

    json_response = ah.list_submissions(None, None, None)
    assert json.loads(json_response.get_data().decode("utf-8"))['total'] == 1
    assert json.loads(json_response.get_data().decode("utf-8"))['submissions'][0]['status'] == "ready"
    delete_models(database, [user, sub, job])

def test_list_submissions_failure(database, job_constants, monkeypatch):
    ah = dataactbroker.handlers.accountHandler.AccountHandler(Mock(), InterfaceHolder())

    mock_value = Mock()
    mock_value.getName.return_value = 1
    monkeypatch.setattr(dataactbroker.handlers.accountHandler, 'LoginSession', mock_value)

    user = UserFactory(user_id=1, cgac_code='cgac')
    sub = SubmissionFactory(user_id=1, submission_id=1, number_of_errors=1, cgac_code='cgac')
    add_models(database, [user, sub])

    json_response = ah.list_submissions(None, None, None)
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

    json_response = ah.list_submissions(None, None, None)
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

    json_response = ah.list_submissions(None, None, None)
    assert json.loads(json_response.get_data().decode("utf-8"))['total'] == 1
    assert json.loads(json_response.get_data().decode("utf-8"))['submissions'][0]['status'] == "file_errors"
    delete_models(database, [user, sub, job])
