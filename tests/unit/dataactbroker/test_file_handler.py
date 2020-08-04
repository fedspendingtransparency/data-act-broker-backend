from datetime import date, datetime, timedelta
import io
import json
import os.path
from unittest.mock import Mock
from flask import Flask
from collections import namedtuple

import pytest

import calendar

from dataactcore.aws.s3Handler import S3Handler
from dataactbroker.handlers import fileHandler
from dataactbroker.helpers import filters_helper
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.jobModels import PublishedFilesHistory, Submission
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT, PUBLISH_STATUS_DICT
from dataactcore.utils.responseException import ResponseException
from tests.unit.dataactbroker.utils import add_models, delete_models
from tests.unit.dataactcore.factories.domain import CGACFactory
from tests.unit.dataactcore.factories.job import (JobFactory, SubmissionFactory, PublishHistoryFactory, CommentFactory,
                                                  CertifyHistoryFactory, PublishedFilesHistoryFactory)
from tests.unit.dataactcore.factories.user import UserFactory
from tests.integration.fileTests import AWARD_FILE_T, APPROP_FILE_T, PA_FILE_T

# Mock class for testing create submissions
UploadFile = namedtuple('UploadFile', 'filename')
UploadFile.save = lambda x, y: True


def list_submissions_result(is_fabs=False):
    json_response = fileHandler.list_submissions(1, 10, 'mixed', is_fabs=is_fabs)
    assert json_response.status_code == 200
    return json.loads(json_response.get_data().decode('UTF-8'))


def list_submissions_sort(category, order):
    json_response = fileHandler.list_submissions(1, 10, 'mixed', category, order)
    assert json_response.status_code == 200
    return json.loads(json_response.get_data().decode('UTF-8'))


def mock_create_submission(sess, monkeypatch, request_params):
    with Flask('test-app').app_context():
        mock_request = Mock()
        mock_request.headers = {'Content-Type': 'multipart/form-data'}
        monkeypatch.setattr(mock_request, 'get_json', Mock(return_value=request_params))
        fh = fileHandler.FileHandler(mock_request, is_local=True)
        monkeypatch.setattr(fh, 'finalize', Mock(return_value=True))
        resp = fh.validate_upload_dabs_files()
        new_sub = sess.query(Submission).filter(Submission.submission_id == resp.json['submission_id']).one_or_none()
    return new_sub


@pytest.mark.usefixtures('job_constants')
def test_create_submission_already_pub_mon(database, monkeypatch):
    """ Ensure submission is appropriately populated upon creation with monthly submissions already published """
    sess = database.session

    cgac = CGACFactory(cgac_code='020', agency_name='Age')
    user1 = UserFactory(user_id=1, name='Oliver Queen', website_admin=True)
    pub_mon1_sub = SubmissionFactory(user_id=1, number_of_warnings=1, cgac_code=cgac.cgac_code,
                                     reporting_fiscal_period=4, reporting_fiscal_year=2010,
                                     publish_status_id=2, is_quarter_format=False)
    pub_mon2_sub = SubmissionFactory(user_id=1, number_of_warnings=1, cgac_code=cgac.cgac_code,
                                     reporting_fiscal_period=5, reporting_fiscal_year=2010,
                                     publish_status_id=3, is_quarter_format=False)
    sess.add_all([user1, cgac, pub_mon1_sub, pub_mon2_sub])
    sess.commit()

    monkeypatch.setattr(fileHandler, 'g', Mock(user=user1))

    # Making a new monthly sub in the same period
    request_params = {
        'cgac_code': cgac.cgac_code,
        'frec_code': None,
        'is_quarter': False,
        'reporting_period_start_date': '01/2010',
        'reporting_period_end_date': '01/2010',
        '_files': {'award_financial': UploadFile(AWARD_FILE_T[1]),
                   'appropriations': UploadFile(APPROP_FILE_T[1]),
                   'program_activity': UploadFile(PA_FILE_T[1])}
    }
    new_mon_same_sub = mock_create_submission(sess, monkeypatch, request_params)

    # Making a new monthly sub in a different period
    request_params = {
        'cgac_code': cgac.cgac_code,
        'frec_code': None,
        'is_quarter': False,
        'reporting_period_start_date': '03/2010',
        'reporting_period_end_date': '03/2010',
        '_files': {'award_financial': UploadFile(AWARD_FILE_T[1]),
                   'appropriations': UploadFile(APPROP_FILE_T[1]),
                   'program_activity': UploadFile(PA_FILE_T[1])}
    }
    new_mon_diff_sub = mock_create_submission(sess, monkeypatch, request_params)

    # Making a new quarterly sub in the same quarter
    request_params = {
        'cgac_code': cgac.cgac_code,
        'frec_code': None,
        'is_quarter': True,
        'reporting_period_start_date': '01/2010',
        'reporting_period_end_date': '03/2010',
        '_files': {'award_financial': UploadFile(AWARD_FILE_T[1]),
                   'appropriations': UploadFile(APPROP_FILE_T[1]),
                   'program_activity': UploadFile(PA_FILE_T[1])}
    }
    new_qtr_same_sub = mock_create_submission(sess, monkeypatch, request_params)

    # Making a new quarterly sub in a different quarter
    request_params = {
        'cgac_code': cgac.cgac_code,
        'frec_code': None,
        'is_quarter': True,
        'reporting_period_start_date': '04/2010',
        'reporting_period_end_date': '06/2010',
        '_files': {'award_financial': UploadFile(AWARD_FILE_T[1]),
                   'appropriations': UploadFile(APPROP_FILE_T[1]),
                   'program_activity': UploadFile(PA_FILE_T[1])}
    }
    new_qtr_diff_sub = mock_create_submission(sess, monkeypatch, request_params)

    # monthly same period -> published monthly sub
    assert new_mon_same_sub.published_submission_ids == [pub_mon1_sub.submission_id]
    assert new_mon_same_sub.test_submission is True
    # monthly different period unaffected
    assert new_mon_diff_sub.published_submission_ids == []
    assert new_mon_diff_sub.test_submission is False
    # quarterly same quarter -> multiple published monthly subs
    assert new_qtr_same_sub.published_submission_ids == [pub_mon1_sub.submission_id, pub_mon2_sub.submission_id]
    assert new_qtr_same_sub.test_submission is True
    # quarterly different quarter unaffected
    assert new_qtr_diff_sub.published_submission_ids == []
    assert new_qtr_diff_sub.test_submission is False


@pytest.mark.usefixtures('job_constants')
def test_create_submission_already_pub_qtr(database, monkeypatch):
    """ Ensure submission is appropriately populated upon creation with monthly submissions already published """
    sess = database.session

    cgac = CGACFactory(cgac_code='020', agency_name='Age')
    user1 = UserFactory(user_id=1, name='Oliver Queen', website_admin=True)
    pub_qtr_sub = SubmissionFactory(user_id=1, number_of_warnings=1, cgac_code=cgac.cgac_code,
                                    reporting_fiscal_period=6, reporting_fiscal_year=2010,
                                    publish_status_id=2, is_quarter_format=True)
    sess.add_all([user1, cgac, pub_qtr_sub])
    sess.commit()

    monkeypatch.setattr(fileHandler, 'g', Mock(user=user1))

    # Making a new monthly sub in the same period
    request_params = {
        'cgac_code': cgac.cgac_code,
        'frec_code': None,
        'is_quarter': False,
        'reporting_period_start_date': '01/2010',
        'reporting_period_end_date': '01/2010',
        '_files': {'award_financial': UploadFile(AWARD_FILE_T[1]),
                   'appropriations': UploadFile(APPROP_FILE_T[1]),
                   'program_activity': UploadFile(PA_FILE_T[1])}
    }
    new_mon_same_sub = mock_create_submission(sess, monkeypatch, request_params)

    # Making a new monthly sub in a different period
    request_params = {
        'cgac_code': cgac.cgac_code,
        'frec_code': None,
        'is_quarter': False,
        'reporting_period_start_date': '04/2010',
        'reporting_period_end_date': '04/2010',
        '_files': {'award_financial': UploadFile(AWARD_FILE_T[1]),
                   'appropriations': UploadFile(APPROP_FILE_T[1]),
                   'program_activity': UploadFile(PA_FILE_T[1])}
    }
    new_mon_diff_sub = mock_create_submission(sess, monkeypatch, request_params)

    # Making a new quarterly sub in the same quarter
    request_params = {
        'cgac_code': cgac.cgac_code,
        'frec_code': None,
        'is_quarter': True,
        'reporting_period_start_date': '01/2010',
        'reporting_period_end_date': '03/2010',
        '_files': {'award_financial': UploadFile(AWARD_FILE_T[1]),
                   'appropriations': UploadFile(APPROP_FILE_T[1]),
                   'program_activity': UploadFile(PA_FILE_T[1])}
    }
    new_qtr_same_sub = mock_create_submission(sess, monkeypatch, request_params)

    # Making a new quarterly sub in a different quarter
    request_params = {
        'cgac_code': cgac.cgac_code,
        'frec_code': None,
        'is_quarter': True,
        'reporting_period_start_date': '04/2010',
        'reporting_period_end_date': '06/2010',
        '_files': {'award_financial': UploadFile(AWARD_FILE_T[1]),
                   'appropriations': UploadFile(APPROP_FILE_T[1]),
                   'program_activity': UploadFile(PA_FILE_T[1])}
    }
    new_qtr_diff_sub = mock_create_submission(sess, monkeypatch, request_params)

    # monthly same period -> published quarter sub
    assert new_mon_same_sub.published_submission_ids == [pub_qtr_sub.submission_id]
    assert new_mon_same_sub.test_submission is True
    # monthly different period unaffected
    assert new_mon_diff_sub.published_submission_ids == []
    assert new_mon_diff_sub.test_submission is False
    # quarterly same quarter -> published quarter sub
    assert new_qtr_same_sub.published_submission_ids == [pub_qtr_sub.submission_id]
    assert new_qtr_same_sub.test_submission is True
    # quarterly different quarter unaffected
    assert new_qtr_diff_sub.published_submission_ids == []
    assert new_qtr_diff_sub.test_submission is False


@pytest.mark.usefixtures('job_constants')
def test_list_submissions_sort_success(database, monkeypatch):
    user1 = UserFactory(user_id=1, name='Oliver Queen', website_admin=True)
    user2 = UserFactory(user_id=2, name='Barry Allen')
    sub1 = SubmissionFactory(user_id=1, submission_id=1, number_of_warnings=1, reporting_start_date=date(2010, 1, 1),
                             publish_status_id=1, is_quarter_format=False)
    sub2 = SubmissionFactory(user_id=1, submission_id=2, number_of_warnings=1, reporting_start_date=date(2010, 1, 2),
                             publish_status_id=1, is_quarter_format=False)
    sub3 = SubmissionFactory(user_id=2, submission_id=3, number_of_warnings=1, reporting_start_date=date(2010, 1, 3),
                             publish_status_id=1, is_quarter_format=False)
    sub4 = SubmissionFactory(user_id=2, submission_id=4, number_of_warnings=1, reporting_start_date=date(2010, 1, 4),
                             publish_status_id=1, is_quarter_format=True)
    sub5 = SubmissionFactory(user_id=2, submission_id=5, number_of_warnings=1, reporting_start_date=date(2010, 1, 5),
                             publish_status_id=1, is_quarter_format=True)
    add_models(database, [user1, user2, sub1, sub2, sub3, sub4, sub5])

    monkeypatch.setattr(filters_helper, 'g', Mock(user=user1))
    result = list_submissions_sort('reporting_start', 'desc')
    assert result['total'] == 5
    sub = result['submissions'][0]
    index = 0
    for subit in result['submissions']:
        index += 1
        assert subit['reporting_start_date'] <= sub['reporting_start_date']
        sub = subit

    result = list_submissions_sort('reporting_start', 'asc')
    assert result['total'] == 5
    sub = result['submissions'][0]
    for subit in result['submissions']:
        assert subit['reporting_start_date'] >= sub['reporting_start_date']
        sub = subit

    result = list_submissions_sort('reporting_end', 'desc')
    assert result['total'] == 5
    sub = result['submissions'][0]
    index = 0
    for subit in result['submissions']:
        index += 1
        assert subit['reporting_end_date'] <= sub['reporting_end_date']
        sub = subit

    result = list_submissions_sort('reporting_end', 'asc')
    assert result['total'] == 5
    sub = result['submissions'][0]
    for subit in result['submissions']:
        assert subit['reporting_end_date'] >= sub['reporting_end_date']
        sub = subit

    result = list_submissions_sort('submitted_by', 'asc')
    assert result['total'] == 5
    sub = result['submissions'][0]
    for subit in result['submissions']:
        assert subit['user']['name'] >= sub['user']['name']
        sub = subit

    result = list_submissions_sort('submitted_by', 'desc')
    assert result['total'] == 5
    sub = result['submissions'][0]
    for subit in result['submissions']:
        assert subit['user']['name'] <= sub['user']['name']
        sub = subit

    result = list_submissions_sort('sub_id', 'desc')
    assert result['total'] == 5
    sub = result['submissions'][0]
    for subit in result['submissions']:
        assert subit['submission_id'] <= sub['submission_id']
        sub = subit

    result = list_submissions_sort('quarterly_submission', 'desc')
    assert result['total'] == 5
    for subit in result['submissions'][:2]:
        assert subit['quarterly_submission'] is True
    for subit in result['submissions'][2:]:
        assert subit['quarterly_submission'] is False

    delete_models(database, [user1, user2, sub1, sub2, sub3, sub4, sub5])


@pytest.mark.usefixtures('job_constants')
def test_list_submissions_success(database, monkeypatch):
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, number_of_warnings=1, publish_status_id=1)
    add_models(database, [user, sub])

    monkeypatch.setattr(filters_helper, 'g', Mock(user=user))
    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == 'validation_successful_warnings'
    delete_models(database, [user, sub])

    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=1)
    job = JobFactory(submission_id=1, job_status_id=JOB_STATUS_DICT['finished'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'])
    add_models(database, [user, sub, job])

    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == 'validation_successful'
    delete_models(database, [user, sub, job])

    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=1)
    job = JobFactory(submission_id=1, job_status_id=JOB_STATUS_DICT['running'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'])
    add_models(database, [user, sub, job])

    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == 'running'
    delete_models(database, [user, sub, job])

    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=1)
    job = JobFactory(submission_id=1, job_status_id=JOB_STATUS_DICT['waiting'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'])
    add_models(database, [user, sub, job])

    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == 'waiting'
    delete_models(database, [user, sub, job])

    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=1)
    job = JobFactory(submission_id=1, job_status_id=JOB_STATUS_DICT['ready'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'])
    add_models(database, [user, sub, job])

    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == 'ready'
    delete_models(database, [user, sub, job])

    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=1, d2_submission=True,
                            reporting_start_date=None)
    job = JobFactory(submission_id=1, job_status_id=JOB_STATUS_DICT['ready'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'])
    add_models(database, [user, sub, job])

    result = list_submissions_result(is_fabs=True)
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == 'ready'
    assert result['submissions'][0]['time_period'] == ''
    delete_models(database, [user, sub, job])

    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=2, d2_submission=True,
                            reporting_start_date=None, certified=False)
    job = JobFactory(submission_id=1, job_status_id=JOB_STATUS_DICT['finished'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'])
    add_models(database, [user, sub, job])

    result = list_submissions_result(is_fabs=True)
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == "published"
    assert result['submissions'][0]['time_period'] == ""
    delete_models(database, [user, sub, job])

    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=2, d2_submission=True,
                            reporting_start_date=None, certified=True)
    job = JobFactory(submission_id=1, job_status_id=JOB_STATUS_DICT['finished'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'])
    add_models(database, [user, sub, job])

    result = list_submissions_result(is_fabs=True)
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == "certified"
    assert result['submissions'][0]['time_period'] == ""
    delete_models(database, [user, sub, job])

    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=3, d2_submission=True,
                            reporting_start_date=None, certified=True)
    job = JobFactory(submission_id=1, job_status_id=JOB_STATUS_DICT['finished'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'])
    add_models(database, [user, sub, job])

    result = list_submissions_result(is_fabs=True)
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == "updated"
    assert result['submissions'][0]['time_period'] == ""
    delete_models(database, [user, sub, job])


@pytest.mark.usefixtures('job_constants')
def test_list_submissions_failure(database, monkeypatch):
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, number_of_errors=1, publish_status_id=1)
    add_models(database, [user, sub])

    monkeypatch.setattr(filters_helper, 'g', Mock(user=user))
    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == 'validation_errors'
    delete_models(database, [user, sub])

    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=1)
    job = JobFactory(submission_id=1, job_status_id=JOB_STATUS_DICT['failed'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'])
    add_models(database, [user, sub, job])

    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == 'failed'
    delete_models(database, [user, sub, job])

    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=1)
    job = JobFactory(submission_id=1, job_status_id=JOB_STATUS_DICT['invalid'],
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['award'])
    add_models(database, [user, sub, job])

    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == 'file_errors'
    delete_models(database, [user, sub, job])


@pytest.mark.usefixtures('job_constants')
def test_list_submissions_detached(database, monkeypatch):
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=1)
    d2_sub = SubmissionFactory(user_id=1, submission_id=2, d2_submission=True, publish_status_id=1)
    add_models(database, [user, sub, d2_sub])

    monkeypatch.setattr(filters_helper, 'g', Mock(user=user))
    result = list_submissions_result()
    fabs_result = list_submissions_result(is_fabs=True)

    assert result['total'] == 1
    assert result['submissions'][0]['submission_id'] == sub.submission_id
    assert fabs_result['total'] == 1
    assert fabs_result['submissions'][0]['submission_id'] == d2_sub.submission_id
    delete_models(database, [user, sub, d2_sub])


@pytest.mark.usefixtures('user_constants')
@pytest.mark.usefixtures('job_constants')
def test_list_submissions_permissions(database, monkeypatch):
    """ Verify that the user must be in the same CGAC group, the submission's owner, or website admin to see the
        submission
    """
    cgac1, cgac2 = CGACFactory(), CGACFactory()
    user1, user2 = UserFactory.with_cgacs(cgac1), UserFactory()
    database.session.add_all([cgac1, cgac2, user1, user2])
    database.session.commit()
    sub = SubmissionFactory(user_id=user2.user_id, cgac_code=cgac2.cgac_code, publish_status_id=1)
    database.session.add(sub)
    database.session.commit()

    monkeypatch.setattr(filters_helper, 'g', Mock(user=user1))
    assert list_submissions_result()['total'] == 0

    user1.affiliations[0].cgac = cgac2
    database.session.commit()
    assert list_submissions_result()['total'] == 1
    user1.affiliations = []
    database.session.commit()
    assert list_submissions_result()['total'] == 0

    sub.user_id = user1.user_id
    database.session.commit()
    assert list_submissions_result()['total'] == 1
    sub.user_id = user2.user_id
    database.session.commit()
    assert list_submissions_result()['total'] == 0

    user1.website_admin = True
    database.session.commit()
    assert list_submissions_result()['total'] == 1


@pytest.mark.usefixtures('job_constants', 'broker_files_tmp_dir')
def test_comments(database):
    """ Verify that we can add, retrieve, and update submission comments. Not quite a unit test as it covers a few
        functions in sequence.
    """
    sub1, sub2 = SubmissionFactory(publish_status_id=PUBLISH_STATUS_DICT['published']), SubmissionFactory()
    database.session.add_all([sub1, sub2])
    database.session.commit()

    # Write some comments
    result = fileHandler.update_submission_comments(sub1, {'B': 'BBBBBB', 'E': 'EEEEEE', 'FABS': 'This wont show up'},
                                                    CONFIG_BROKER['local'])
    assert result.status_code == 200
    # Make sure submission updates if it's published
    assert sub1.publish_status_id == PUBLISH_STATUS_DICT['updated']

    result = fileHandler.update_submission_comments(sub2, {'A': 'Submission2'}, CONFIG_BROKER['local'])
    assert result.status_code == 200

    # Check the comments
    result = fileHandler.get_submission_comments(sub1)
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

    # Replace the comments
    result = fileHandler.update_submission_comments(sub1, {'A': 'AAAAAA', 'E': 'E2E2E2'}, CONFIG_BROKER['local'])
    assert result.status_code == 200

    # Verify the change worked
    result = fileHandler.get_submission_comments(sub1)
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


@pytest.mark.usefixtures('job_constants', 'broker_files_tmp_dir')
def test_get_comments_file(database):
    """ Test getting a URL for the comments file """

    sub1, sub2 = SubmissionFactory(), SubmissionFactory()
    database.session.add_all([sub1, sub2])
    database.session.commit()

    # Write some comments
    fileHandler.update_submission_comments(sub1, {'B': 'BBBBBB', 'E': 'EEEEEE'}, CONFIG_BROKER['local'])

    result = fileHandler.get_comments_file(sub1, CONFIG_BROKER['local'])
    assert result.status_code == 200
    result = json.loads(result.get_data().decode('UTF-8'))
    assert 'submission_{}_comments.csv'.format(sub1.submission_id) in result['url']

    # If it's a submission with no comments, it should return an error
    result = fileHandler.get_comments_file(sub2, CONFIG_BROKER['local'])
    assert result.status_code == 400

good_dates = [
    ('04/2016', '05/2016', False, None),
    ('07/2014', '07/2014', False, None),
    ('01/2010', '03/2010', False, None),
    ('10/2017', '12/2017', True, None),
    ('04/2016', None, False, SubmissionFactory(
        reporting_start_date=datetime.strptime('09/2016', '%m/%Y').date(),
        reporting_end_date=datetime.strptime('09/30/2016', '%m/%d/%Y').date())),
    (None, '07/2014', None, SubmissionFactory(
        reporting_start_date=datetime.strptime('08/2013', '%m/%Y').date(),
        reporting_end_date=datetime.strptime('09/30/2016', '%m/%d/%Y').date())),
    ('01/2010', '03/2010', True, SubmissionFactory(is_quarter_format=False)),
    (None, None, None, SubmissionFactory(
        reporting_start_date=datetime.strptime('09/2016', '%m/%Y').date(),
        reporting_end_date=datetime.strptime('09/30/2016', '%m/%d/%Y').date()
    )),
    (None, None, None, SubmissionFactory(
        is_quarter_format=True,
        reporting_start_date=datetime.strptime('10/2016', '%m/%Y').date(),
        reporting_end_date=datetime.strptime('12/31/2016', '%m/%d/%Y').date()
    ))
]


@pytest.mark.parametrize('start_date, end_date, quarter_flag, submission', good_dates)
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
        test_date = datetime.strptime(end_date, date_format).date()
        test_date = datetime.strptime(
                        str(test_date.year) + '/' +
                        str(test_date.month) + '/' +
                        str(calendar.monthrange(test_date.year, test_date.month)[1]),
                        '%Y/%m/%d'
                    ).date()
        assert output_end_date == test_date

bad_dates = [
    ('04/2016', '05/2016', True, SubmissionFactory()),
    ('08/2016', '11/2016', True, SubmissionFactory()),
    ('07/2014', '06/2014', False, None),
    ('11/2010', '03/2010', False, None),
    ('10/2017', '12/xyz', True, None),
    ('01/2016', '07/2016', True, None),
    (None, '01/1930', False, SubmissionFactory())
]


@pytest.mark.parametrize('start_date, end_date, quarter_flag, submission', bad_dates)
def test_submission_bad_dates(start_date, end_date, quarter_flag, submission):
    """Verify that submission date checks fail on bad input"""
    # all dates must be in mm/yyyy format
    # quarterly submissions:
    # - can span a single quarter only
    # - must end with month = 3, 6, 9, or 12
    fh = fileHandler.FileHandler(Mock())
    with pytest.raises(ResponseException):
        fh.check_submission_dates(start_date, end_date, quarter_flag, submission)


@pytest.mark.usefixtures('job_constants')
def test_submission_report_url_local(monkeypatch, tmpdir, database):
    old_sub = SubmissionFactory(submission_id=4, d2_submission=False)
    new_sub = SubmissionFactory(submission_id=5, d2_submission=False)
    old_job = JobFactory(submission_id=4, job_status_id=JOB_STATUS_DICT['finished'],
                         job_type_id=JOB_TYPE_DICT['validation'], file_type_id=FILE_TYPE_DICT['award_financial'],
                         updated_at='2017-01-01')
    new_job = JobFactory(submission_id=5, job_status_id=JOB_STATUS_DICT['finished'],
                         job_type_id=JOB_TYPE_DICT['validation'], file_type_id=FILE_TYPE_DICT['award_financial'],
                         updated_at='2020-08-01')
    add_models(database, [old_sub, old_job, new_sub, new_job])

    file_path = str(tmpdir) + os.path.sep
    monkeypatch.setattr(fileHandler, 'CONFIG_BROKER', {'local': True, 'broker_files': file_path})

    json_response = fileHandler.submission_report_url(old_sub, True, 'award_financial', 'award')
    url = json.loads(json_response.get_data().decode('utf-8'))['url']
    assert url == os.path.join(file_path, 'submission_4_cross_warning_award_financial_award.csv')

    json_response = fileHandler.submission_report_url(new_sub, True, 'award_financial', 'award')
    url = json.loads(json_response.get_data().decode('utf-8'))['url']
    assert url == os.path.join(file_path, 'submission_5_crossfile_warning_File_C_to_D2_award_financial_award.csv')


@pytest.mark.usefixtures('job_constants')
def test_submission_report_url_s3(monkeypatch, database):
    old_sub = SubmissionFactory(submission_id=4, d2_submission=False)
    new_sub = SubmissionFactory(submission_id=5, d2_submission=False)
    old_job = JobFactory(submission_id=4, job_status_id=JOB_STATUS_DICT['finished'],
                         job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                         file_type_id=FILE_TYPE_DICT['appropriations'], updated_at='2017-01-01')
    new_job = JobFactory(submission_id=5, job_status_id=JOB_STATUS_DICT['finished'],
                         job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                         file_type_id=FILE_TYPE_DICT['appropriations'], updated_at='2020-08-01')
    add_models(database, [old_sub, old_job, new_sub, new_job])

    monkeypatch.setattr(fileHandler, 'CONFIG_BROKER', {'local': False, 'submission_bucket_mapping': 'test/path'})
    s3_url_handler = Mock()
    s3_url_handler.return_value.get_signed_url.return_value = 'some/url/here.csv'
    monkeypatch.setattr(fileHandler, 'S3Handler', s3_url_handler)

    json_response = fileHandler.submission_report_url(old_sub, False, 'appropriations', None)
    url = json.loads(json_response.get_data().decode('utf-8'))['url']
    assert url == 'some/url/here.csv'
    assert s3_url_handler.return_value.get_signed_url.call_args == (
        ('errors', 'submission_4_appropriations_error_report.csv'),
        {'method': 'get_object', 'url_mapping': 'test/path'}
    )
    json_response = fileHandler.submission_report_url(new_sub, False, 'appropriations', None)
    url = json.loads(json_response.get_data().decode('utf-8'))['url']
    assert url == 'some/url/here.csv'
    assert s3_url_handler.return_value.get_signed_url.call_args == (
        ('errors', 'submission_5_File_A_appropriations_error_report.csv'),
        {'method': 'get_object', 'url_mapping': 'test/path'}
    )


def test_build_file_map_string(monkeypatch):
    monkeypatch.setattr(fileHandler, 'CONFIG_BROKER', {'local': False})
    upload_files = []
    file_type_list = ['fabs', 'appropriations', 'award_financial', 'program_activity']
    file_dict = {'fabs': 'fabs_file.csv',
                 'appropriations': 'appropriations.csv',
                 'award_financial': 'award_financial.csv',
                 'program_activity': 'program_activity.csv'}
    monkeypatch.setattr(S3Handler, 'get_timestamped_filename', Mock(side_effect=lambda x: '123_' + x))
    submission = SubmissionFactory(submission_id=3)
    fh = fileHandler.FileHandler({})
    fh.build_file_map(file_dict, file_type_list, upload_files, submission)
    for file in upload_files:
        assert file.upload_name == '3/123_'+file.file_name


def test_build_file_map_file(monkeypatch):
    monkeypatch.setattr(fileHandler, 'CONFIG_BROKER', {'local': False})
    upload_files = []
    file_type_list = ['fabs', 'appropriations', 'award_financial', 'program_activity']
    fabs_file = io.BytesIO(b'something')
    fabs_file.filename = 'fabs.csv'
    approp_file = io.BytesIO(b'something')
    approp_file.filename = 'approp.csv'
    pa_file = io.BytesIO(b'something')
    pa_file.filename = 'pa.csv'
    award_file = io.BytesIO(b'something')
    award_file.filename = 'award.csv'
    file_dict = {'fabs': fabs_file, 'award_financial': award_file, 'program_activity': pa_file,
                 'appropriations': approp_file}
    monkeypatch.setattr(S3Handler, 'get_timestamped_filename', Mock(side_effect=lambda x: '123_' + x))
    submission = SubmissionFactory(submission_id=3)
    fh = fileHandler.FileHandler({})
    fh.build_file_map(file_dict, file_type_list, upload_files, submission)
    for file in upload_files:
        assert file.upload_name == '3/123_'+file.file_name


@pytest.mark.usefixtures('job_constants')
def test_get_upload_file_url_local(database, monkeypatch, tmpdir):
    """ Test getting the url of the uploaded file locally. """
    file_path = str(tmpdir) + os.path.sep
    monkeypatch.setattr(fileHandler, 'CONFIG_BROKER', {'local': True, 'broker_files': file_path})

    # create and insert submission/job
    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    job = JobFactory(submission_id=1, job_status_id=JOB_STATUS_DICT['finished'],
                     job_type_id=JOB_TYPE_DICT['file_upload'], file_type_id=FILE_TYPE_DICT['appropriations'],
                     filename='a/path/to/some_file.csv')
    add_models(database, [sub, job])

    json_response = fileHandler.get_upload_file_url(sub, 'A')
    url = json.loads(json_response.get_data().decode('utf-8'))['url']
    assert url == os.path.join(file_path, 'some_file.csv')


def test_get_upload_file_url_invalid_for_type(database):
    """ Test that a proper error is thrown when a file type that doesn't match the submission is provided to
        get_upload_file_url.
    """
    sub_1 = SubmissionFactory(submission_id=1, d2_submission=False)
    sub_2 = SubmissionFactory(submission_id=2, d2_submission=True)
    add_models(database, [sub_1, sub_2])
    json_response = fileHandler.get_upload_file_url(sub_2, 'A')

    # check invalid type for FABS
    assert json_response.status_code == 400
    response = json.loads(json_response.get_data().decode('utf-8'))
    assert response['message'] == 'Invalid file type for this submission'

    # check invalid type for DABS
    json_response = fileHandler.get_upload_file_url(sub_1, 'FABS')
    assert json_response.status_code == 400
    response = json.loads(json_response.get_data().decode('utf-8'))
    assert response['message'] == 'Invalid file type for this submission'


@pytest.mark.usefixtures('job_constants')
def test_get_upload_file_url_no_file(database):
    """ Test that a proper error is thrown when an upload job doesn't have a file associated with it
        get_upload_file_url.
    """
    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    job = JobFactory(submission_id=1, job_status_id=JOB_STATUS_DICT['finished'],
                     job_type_id=JOB_TYPE_DICT['file_upload'], file_type_id=FILE_TYPE_DICT['appropriations'],
                     filename=None)
    add_models(database, [sub, job])

    json_response = fileHandler.get_upload_file_url(sub, 'A')
    assert json_response.status_code == 400
    response = json.loads(json_response.get_data().decode('utf-8'))
    assert response['message'] == 'No file uploaded or generated for this type'


@pytest.mark.usefixtures('job_constants')
def test_get_upload_file_url_s3(database, monkeypatch):
    """ Test getting the url of the uploaded file non-locally. """
    monkeypatch.setattr(fileHandler, 'CONFIG_BROKER', {'local': False, 'submission_bucket_mapping': 'test/path'})
    s3_url_handler = Mock()
    s3_url_handler.return_value.get_signed_url.return_value = 'some/url/here.csv'
    monkeypatch.setattr(fileHandler, 'S3Handler', s3_url_handler)

    # create and insert submission/job
    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    job = JobFactory(submission_id=1, job_status_id=JOB_STATUS_DICT['finished'],
                     job_type_id=JOB_TYPE_DICT['file_upload'], file_type_id=FILE_TYPE_DICT['appropriations'],
                     filename='1/some_file.csv')
    add_models(database, [sub, job])

    json_response = fileHandler.get_upload_file_url(sub, 'A')
    url = json.loads(json_response.get_data().decode('utf-8'))['url']
    assert url == 'some/url/here.csv'
    assert s3_url_handler.return_value.get_signed_url.call_args == (
        ('1', 'some_file.csv'),
        {'method': 'get_object', 'url_mapping': 'test/path'}
    )


@pytest.mark.usefixtures('job_constants')
def test_move_published_files(database, monkeypatch):
    # set up cgac and submission
    cgac = CGACFactory(cgac_code='zyxwv', agency_name='Test')
    qtr_sub = SubmissionFactory(cgac_code='zyxwv', number_of_errors=0, publish_status_id=1,
                                reporting_fiscal_year=2017, reporting_fiscal_period=6, is_quarter_format=True)
    mon_sub = SubmissionFactory(cgac_code='zyxwv', number_of_errors=0, publish_status_id=1,
                                reporting_fiscal_year=2017, reporting_fiscal_period=2, is_quarter_format=False)
    database.session.add_all([cgac, qtr_sub])
    database.session.commit()

    # set up publish/certify history and jobs based on submission
    sess = database.session
    pub_hist_local = PublishHistoryFactory(submission_id=qtr_sub.submission_id)
    pub_hist_remote_qtr = PublishHistoryFactory(submission_id=qtr_sub.submission_id)
    pub_hist_remote_mon = PublishHistoryFactory(submission_id=qtr_sub.submission_id)
    cert_hist_local = CertifyHistoryFactory(submission_id=qtr_sub.submission_id)
    cert_hist_remote_qtr = CertifyHistoryFactory(submission_id=qtr_sub.submission_id)
    cert_hist_remote_mon = CertifyHistoryFactory(submission_id=qtr_sub.submission_id)

    finished_job = JOB_STATUS_DICT['finished']
    upload_job = JOB_TYPE_DICT['file_upload']
    val_job = JOB_TYPE_DICT['csv_record_validation']
    cross_job = JOB_TYPE_DICT['validation']
    appropriations_job_qtr = JobFactory(submission=qtr_sub, filename='/path/to/appropriations/file_a.csv',
                                        file_type_id=FILE_TYPE_DICT['appropriations'], job_type_id=upload_job,
                                        job_status_id=finished_job)
    val_appropriations_job_qtr = JobFactory(submission=qtr_sub, filename='/path/to/appropriations/file_a.csv',
                                            file_type_id=FILE_TYPE_DICT['appropriations'], job_type_id=val_job,
                                            job_status_id=finished_job)
    prog_act_job_qtr = JobFactory(submission=qtr_sub, filename='/path/to/prog/act/file_b.csv',
                                  file_type_id=FILE_TYPE_DICT['program_activity'], job_type_id=upload_job,
                                  job_status_id=finished_job)
    val_prog_act_job_qtr = JobFactory(submission=qtr_sub, filename='/path/to/prog/act/file_b.csv',
                                      file_type_id=FILE_TYPE_DICT['program_activity'], job_type_id=val_job,
                                      job_status_id=finished_job)
    award_fin_job_qtr = JobFactory(submission=qtr_sub, filename='/path/to/award/fin/file_c.csv',
                                   file_type_id=FILE_TYPE_DICT['award_financial'], job_type_id=upload_job,
                                   job_status_id=finished_job)
    val_award_fin_job_qtr = JobFactory(submission=qtr_sub, filename='/path/to/award/fin/file_c.csv',
                                       file_type_id=FILE_TYPE_DICT['award_financial'], job_type_id=val_job,
                                       job_status_id=finished_job)
    award_proc_job_qtr = JobFactory(submission=qtr_sub, filename='/path/to/award/proc/file_d1.csv',
                                    file_type_id=FILE_TYPE_DICT['award_procurement'], job_type_id=upload_job,
                                    job_status_id=finished_job)
    award_job_qtr = JobFactory(submission=qtr_sub, filename='/path/to/award/file_d2.csv',
                               file_type_id=FILE_TYPE_DICT['award'], job_type_id=upload_job,
                               job_status_id=finished_job)
    cross_job_qtr = JobFactory(submission=qtr_sub, filename='/path/to/award/cross_file.csv',
                               file_type_id=FILE_TYPE_DICT['award_financial'], job_type_id=cross_job,
                               job_status_id=finished_job)
    exec_comp_job_qtr = JobFactory(submission=qtr_sub, filename='/path/to/exec/comp/file_e.csv',
                                   file_type_id=FILE_TYPE_DICT['executive_compensation'], job_type_id=upload_job,
                                   job_status_id=finished_job)
    sub_award_job_qtr = JobFactory(submission=qtr_sub, filename='/path/to/sub/award/file_f.csv',
                                   file_type_id=FILE_TYPE_DICT['sub_award'], job_type_id=upload_job,
                                   job_status_id=finished_job)

    award_fin_narr_qtr = CommentFactory(submission=qtr_sub, comment='Test comment',
                                        file_type_id=FILE_TYPE_DICT['award_financial'])

    appropriations_job_mon = JobFactory(submission=mon_sub, filename='/path/to/appropriations/file_a.csv',
                                        file_type_id=FILE_TYPE_DICT['appropriations'], job_type_id=upload_job,
                                        job_status_id=finished_job)
    val_appropriations_job_mon = JobFactory(submission=mon_sub, filename='/path/to/appropriations/file_a.csv',
                                            file_type_id=FILE_TYPE_DICT['appropriations'], job_type_id=val_job,
                                            job_status_id=finished_job)
    prog_act_job_mon = JobFactory(submission=mon_sub, filename='/path/to/prog/act/file_b.csv',
                                  file_type_id=FILE_TYPE_DICT['program_activity'], job_type_id=upload_job,
                                  job_status_id=finished_job)
    val_prog_act_job_mon = JobFactory(submission=mon_sub, filename='/path/to/prog/act/file_b.csv',
                                      file_type_id=FILE_TYPE_DICT['program_activity'], job_type_id=val_job,
                                      job_status_id=finished_job)
    award_fin_job_mon = JobFactory(submission=mon_sub, filename='/path/to/award/fin/file_c.csv',
                                   file_type_id=FILE_TYPE_DICT['award_financial'], job_type_id=upload_job,
                                   job_status_id=finished_job)
    val_award_fin_job_mon = JobFactory(submission=mon_sub, filename='/path/to/award/fin/file_c.csv',
                                       file_type_id=FILE_TYPE_DICT['award_financial'], job_type_id=val_job,
                                       job_status_id=finished_job)
    award_proc_job_mon = JobFactory(submission=mon_sub, filename='/path/to/award/proc/file_d1.csv',
                                    file_type_id=FILE_TYPE_DICT['award_procurement'], job_type_id=upload_job,
                                    job_status_id=finished_job)
    award_job_mon = JobFactory(submission=mon_sub, filename='/path/to/award/file_d2.csv',
                               file_type_id=FILE_TYPE_DICT['award'], job_type_id=upload_job,
                               job_status_id=finished_job)
    cross_job_mon = JobFactory(submission=mon_sub, filename='/path/to/award/cross_file.csv',
                               file_type_id=FILE_TYPE_DICT['award_financial'], job_type_id=cross_job,
                               job_status_id=finished_job)
    exec_comp_job_mon = JobFactory(submission=mon_sub, filename='/path/to/exec/comp/file_e.csv',
                                   file_type_id=FILE_TYPE_DICT['executive_compensation'], job_type_id=upload_job,
                                   job_status_id=finished_job)
    sub_award_job_mon = JobFactory(submission=mon_sub, filename='/path/to/sub/award/file_f.csv',
                                   file_type_id=FILE_TYPE_DICT['sub_award'], job_type_id=upload_job,
                                   job_status_id=finished_job)

    database.session.add_all([pub_hist_local, pub_hist_remote_qtr, cert_hist_local, cert_hist_remote_qtr,
                              appropriations_job_qtr, val_appropriations_job_qtr, prog_act_job_qtr,
                              val_prog_act_job_qtr, award_fin_job_qtr, val_award_fin_job_qtr, award_proc_job_qtr,
                              award_job_qtr, cross_job_qtr, exec_comp_job_qtr, sub_award_job_qtr,
                              award_fin_narr_qtr, pub_hist_remote_mon, cert_hist_remote_mon, appropriations_job_mon,
                              val_appropriations_job_mon, prog_act_job_mon, val_prog_act_job_mon, award_fin_job_mon,
                              val_award_fin_job_mon, award_proc_job_mon, award_job_mon, cross_job_mon,
                              exec_comp_job_mon, sub_award_job_mon])
    database.session.commit()

    s3_url_handler = Mock()
    monkeypatch.setattr(fileHandler, 'S3Handler', s3_url_handler)
    monkeypatch.setattr(fileHandler, 'CONFIG_BROKER', {'aws_bucket': 'original_bucket',
                                                       'certified_bucket': 'cert_bucket'})
    monkeypatch.setattr(fileHandler, 'CONFIG_SERVICES', {'error_report_path': '/path/to/error/reports/'})

    fh = fileHandler.FileHandler(Mock())

    # test local publication
    fh.move_published_files(qtr_sub, pub_hist_local, cert_hist_local.certify_history_id, True)
    local_id = pub_hist_local.publish_history_id

    # make sure we have the right number of history entries
    all_local_certs = sess.query(PublishedFilesHistory).filter_by(publish_history_id=local_id).all()
    assert len(all_local_certs) == 11

    c_cert_hist = sess.query(PublishedFilesHistory).\
        filter_by(publish_history_id=local_id, file_type_id=FILE_TYPE_DICT['award_financial']).one()
    assert c_cert_hist.filename == '/path/to/award/fin/file_c.csv'
    expected_filename = '/path/to/error/reports/submission_{}_File_C_award_financial_warning_report.csv'.\
        format(qtr_sub.submission_id)
    assert c_cert_hist.warning_filename == expected_filename
    assert c_cert_hist.comment == 'Test comment'

    # cross-file warnings
    warning_cert_hist = sess.query(PublishedFilesHistory).filter_by(publish_history_id=local_id, file_type=None).all()
    assert len(warning_cert_hist) == 4
    assert warning_cert_hist[0].comment is None

    warning_cert_hist_files = [hist.warning_filename for hist in warning_cert_hist]
    assert '/path/to/error/reports/submission_{}_crossfile_warning_File_A_to_B_appropriations_program_activity.csv'.\
        format(qtr_sub.submission_id) in warning_cert_hist_files

    # test remote publication - quarter
    fh.move_published_files(qtr_sub, pub_hist_remote_qtr, cert_hist_remote_qtr.certify_history_id, False)
    remote_id = pub_hist_remote_qtr.publish_history_id

    c_cert_hist = sess.query(PublishedFilesHistory). \
        filter_by(publish_history_id=remote_id, file_type_id=FILE_TYPE_DICT['award_financial']).one()
    assert c_cert_hist.filename == 'zyxwv/2017/Q2/{}/file_c.csv'.format(remote_id)
    assert c_cert_hist.warning_filename == 'zyxwv/2017/Q2/{}/submission_{}_File_C_award_financial_warning_report.csv'. \
        format(remote_id, qtr_sub.submission_id)

    # test remote publication - month
    fh.move_published_files(mon_sub, pub_hist_remote_mon, cert_hist_remote_mon.certify_history_id, False)
    remote_id = pub_hist_remote_mon.publish_history_id

    c_cert_hist = sess.query(PublishedFilesHistory). \
        filter_by(publish_history_id=remote_id, file_type_id=FILE_TYPE_DICT['award_financial']).one()
    assert c_cert_hist.filename == 'zyxwv/2017/P02/{}/file_c.csv'.format(remote_id)
    assert c_cert_hist.warning_filename == 'zyxwv/2017/P02/{}/submission_{}_File_C_award_financial_warning_report.csv'.\
        format(remote_id, mon_sub.submission_id)


@pytest.mark.usefixtures('job_constants')
def test_list_history(database):
    # set up submission
    sub = SubmissionFactory()
    database.session.add(sub)
    database.session.commit()

    # set up publish history, make sure the empty one comes last in the list
    cert_hist_empty = CertifyHistoryFactory(submission=sub, created_at=datetime.utcnow() - timedelta(days=1))
    cert_hist = CertifyHistoryFactory(submission=sub)
    pub_hist = PublishHistoryFactory(submission=sub)
    database.session.add_all([cert_hist_empty, cert_hist, pub_hist])
    database.session.commit()

    # add some data to published_files_history for the cert_history ID
    cert_history_id = cert_hist.certify_history_id
    pub_history_id = pub_hist.publish_history_id
    sub_id = sub.submission_id
    file_hist_1 = PublishedFilesHistoryFactory(certify_history_id=cert_history_id,
                                               publish_history_id=pub_hist.publish_history_id, submission_id=sub_id,
                                               filename='/path/to/file_a.csv',
                                               warning_filename='/path/to/warning_file_a.csv',
                                               comment='A has a comment',
                                               file_type_id=FILE_TYPE_DICT['appropriations'])
    file_hist_2 = PublishedFilesHistoryFactory(certify_history_id=cert_history_id,
                                               publish_history_id=pub_history_id, submission_id=sub_id,
                                               filename='/path/to/file_d2.csv',
                                               warning_filename=None,
                                               file_type_id=FILE_TYPE_DICT['award'])
    file_hist_3 = PublishedFilesHistoryFactory(certify_history_id=cert_history_id,
                                               publish_history_id=pub_history_id, submission_id=sub_id,
                                               filename=None,
                                               warning_filename='/path/to/warning_file_cross_test.csv',
                                               file_type_id=None)
    database.session.add_all([file_hist_1, file_hist_2, file_hist_3])
    database.session.commit()

    json_response = fileHandler.list_history(sub)
    response_dict = json.loads(json_response.get_data().decode('utf-8'))
    assert len(response_dict['certifications']) == 2
    assert len(response_dict['publications']) == 1

    has_file_list = response_dict['certifications'][0]
    empty_file_list = response_dict['certifications'][1]
    pub_list = response_dict['publications'][0]

    # asserts for certification with files associated
    assert len(has_file_list['certified_files']) == 4
    assert has_file_list['certified_files'][0]['is_warning'] is False
    assert has_file_list['certified_files'][0]['filename'] == 'file_a.csv'
    assert has_file_list['certified_files'][0]['comment'] == 'A has a comment'

    assert has_file_list['certified_files'][1]['is_warning']
    assert has_file_list['certified_files'][1]['comment'] is None

    # asserts for certification without files associated
    assert len(empty_file_list['certified_files']) == 0

    # asserts for publications
    assert len(pub_list['published_files']) == 4
    assert pub_list['published_files'][0]['is_warning'] is False
    assert pub_list['published_files'][0]['filename'] == 'file_a.csv'
    assert pub_list['published_files'][0]['comment'] == 'A has a comment'


def test_file_history_url(database, monkeypatch):
    sub = SubmissionFactory()
    database.session.add(sub)
    database.session.commit()

    # set up certify history so it works
    cert_hist = CertifyHistoryFactory(submission=sub)
    pub_hist = PublishHistoryFactory(submission=sub)
    database.session.add_all([cert_hist, pub_hist])
    database.session.commit()

    file_hist = PublishedFilesHistoryFactory(certify_history_id=cert_hist.certify_history_id,
                                             publish_history_id=pub_hist.publish_history_id,
                                             submission_id=sub.submission_id, filename='/path/to/file_d2.csv',
                                             warning_filename='/path/to/warning_file_cross.csv',
                                             comment=None, file_type_id=None)
    database.session.add(file_hist)
    database.session.commit()

    s3_url_handler = Mock()
    s3_url_handler.return_value.get_signed_url.return_value = 'some/url/here.csv'
    monkeypatch.setattr(fileHandler, 'S3Handler', s3_url_handler)

    # checking for local response to non-warning file
    json_response = fileHandler.file_history_url(sub, file_hist.published_files_history_id, False, True)
    url = json.loads(json_response.get_data().decode('utf-8'))['url']
    assert url == '/path/to/file_d2.csv'

    # local response to warning file
    json_response = fileHandler.file_history_url(sub, file_hist.published_files_history_id, True, True)
    url = json.loads(json_response.get_data().decode('utf-8'))['url']
    assert url == '/path/to/warning_file_cross.csv'

    # generic test to make sure it's reaching the s3 handler properly
    json_response = fileHandler.file_history_url(sub, file_hist.published_files_history_id, False, False)
    url = json.loads(json_response.get_data().decode('utf-8'))['url']
    assert url == 'some/url/here.csv'


def test_get_status_invalid_type(database):
    """ Test get status function for all versions of an "invalid" file type """
    sub_1 = SubmissionFactory(submission_id=1, d2_submission=False)
    sub_2 = SubmissionFactory(submission_id=2, d2_submission=True)

    database.session.add_all([sub_1, sub_2])
    database.session.commit()

    # Getting fabs for non-fabs submissions
    json_response = fileHandler.get_status(sub_1, 'fabs')
    assert json_response.status_code == 400
    json_content = json.loads(json_response.get_data().decode('UTF-8'))
    assert json_content['message'] == 'fabs is not a valid file type for this submission'

    # Getting award for non-dabs submissions
    json_response = fileHandler.get_status(sub_2, 'award')
    assert json_response.status_code == 400
    json_content = json.loads(json_response.get_data().decode('UTF-8'))
    assert json_content['message'] == 'award is not a valid file type for this submission'

    # Getting completely not allowed type
    json_response = fileHandler.get_status(sub_1, 'approp')
    assert json_response.status_code == 400
    json_content = json.loads(json_response.get_data().decode('UTF-8'))
    assert json_content['message'] == 'approp is not a valid file type'


@pytest.mark.usefixtures('job_constants')
def test_get_status_fabs(database):
    """ Test get status function for a fabs submission """
    sess = database.session

    sub = SubmissionFactory(submission_id=1, d2_submission=True)
    job_up = JobFactory(submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['file_upload'],
                        file_type_id=FILE_TYPE_DICT['fabs'], job_status_id=JOB_STATUS_DICT['finished'],
                        number_of_errors=0, number_of_warnings=0)
    job_val = JobFactory(submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                         file_type_id=FILE_TYPE_DICT['fabs'], job_status_id=JOB_STATUS_DICT['finished'],
                         number_of_errors=0, number_of_warnings=4)

    sess.add_all([sub, job_up, job_val])
    sess.commit()

    json_response = fileHandler.get_status(sub)
    assert json_response.status_code == 200
    json_content = json.loads(json_response.get_data().decode('UTF-8'))
    assert json_content['fabs'] == {'status': 'finished', 'has_errors': False, 'has_warnings': True, 'message': ''}


@pytest.mark.usefixtures('job_constants')
def test_get_status_dabs(database):
    """ Test get status function for a dabs submission, including all possible statuses and case insensitivity """
    sess = database.session

    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    upload_job = JOB_TYPE_DICT['file_upload']
    validation_job = JOB_TYPE_DICT['csv_record_validation']
    finished_status = JOB_STATUS_DICT['finished']

    # Completed, warnings, errors
    job_1_up = JobFactory(submission_id=sub.submission_id, job_type_id=upload_job,
                          file_type_id=FILE_TYPE_DICT['appropriations'], job_status_id=finished_status,
                          number_of_errors=0, number_of_warnings=0, error_message=None)
    job_1_val = JobFactory(submission_id=sub.submission_id, job_type_id=validation_job,
                           file_type_id=FILE_TYPE_DICT['appropriations'], job_status_id=finished_status,
                           number_of_errors=10, number_of_warnings=4, error_message=None)
    # Invalid upload
    job_2_up = JobFactory(submission_id=sub.submission_id, job_type_id=upload_job,
                          file_type_id=FILE_TYPE_DICT['program_activity'], job_status_id=JOB_STATUS_DICT['invalid'],
                          number_of_errors=0, number_of_warnings=0, error_message=None)
    job_2_val = JobFactory(submission_id=sub.submission_id, job_type_id=validation_job,
                           file_type_id=FILE_TYPE_DICT['program_activity'], job_status_id=JOB_STATUS_DICT['waiting'],
                           number_of_errors=0, number_of_warnings=0, error_message=None)
    # Validating
    job_3_up = JobFactory(submission_id=sub.submission_id, job_type_id=upload_job,
                          file_type_id=FILE_TYPE_DICT['award_financial'], job_status_id=finished_status,
                          number_of_errors=0, number_of_warnings=0, error_message=None)
    job_3_val = JobFactory(submission_id=sub.submission_id, job_type_id=validation_job,
                           file_type_id=FILE_TYPE_DICT['award_financial'], job_status_id=JOB_STATUS_DICT['running'],
                           number_of_errors=0, number_of_warnings=0, error_message=None)
    # Uploading
    job_4_up = JobFactory(submission_id=sub.submission_id, job_type_id=upload_job,
                          file_type_id=FILE_TYPE_DICT['award'], job_status_id=JOB_STATUS_DICT['running'],
                          number_of_errors=0, number_of_warnings=0, error_message=None)
    job_4_val = JobFactory(submission_id=sub.submission_id, job_type_id=validation_job,
                           file_type_id=FILE_TYPE_DICT['award'], job_status_id=JOB_STATUS_DICT['ready'],
                           number_of_errors=0, number_of_warnings=0, error_message=None)
    # Invalid on validation
    job_5_up = JobFactory(submission_id=sub.submission_id, job_type_id=upload_job,
                          file_type_id=FILE_TYPE_DICT['award_procurement'], job_status_id=finished_status,
                          number_of_errors=0, number_of_warnings=0, error_message=None)
    job_5_val = JobFactory(submission_id=sub.submission_id, job_type_id=validation_job,
                           file_type_id=FILE_TYPE_DICT['award_procurement'], job_status_id=JOB_STATUS_DICT['invalid'],
                           number_of_errors=0, number_of_warnings=0, error_message=None)
    # Failed
    job_6_up = JobFactory(submission_id=sub.submission_id, job_type_id=upload_job,
                          file_type_id=FILE_TYPE_DICT['executive_compensation'],
                          job_status_id=JOB_STATUS_DICT['failed'], number_of_errors=0, number_of_warnings=0,
                          error_message='test message')
    # Ready
    job_7_up = JobFactory(submission_id=sub.submission_id, job_type_id=upload_job,
                          file_type_id=FILE_TYPE_DICT['sub_award'], job_status_id=JOB_STATUS_DICT['ready'],
                          number_of_errors=0, number_of_warnings=0, error_message=None)
    # Waiting
    job_8_val = JobFactory(submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['validation'], file_type_id=None,
                           job_status_id=JOB_STATUS_DICT['waiting'], number_of_errors=0, number_of_warnings=5,
                           error_message=None)

    sess.add_all([sub, job_1_up, job_1_val, job_2_up, job_2_val, job_3_up, job_3_val, job_4_up, job_4_val, job_5_up,
                  job_5_val, job_6_up, job_7_up, job_8_val])
    sess.commit()

    # Get all statuses
    json_response = fileHandler.get_status(sub)
    assert json_response.status_code == 200
    json_content = json.loads(json_response.get_data().decode('UTF-8'))
    assert len(json_content) == 8
    assert json_content['appropriations'] == {'status': 'finished', 'has_errors': True, 'has_warnings': True,
                                              'message': ''}
    assert json_content['program_activity'] == {'status': 'failed', 'has_errors': True, 'has_warnings': False,
                                                'message': ''}
    assert json_content['award_financial'] == {'status': 'running', 'has_errors': False, 'has_warnings': False,
                                               'message': ''}
    assert json_content['award'] == {'status': 'uploading', 'has_errors': False, 'has_warnings': False, 'message': ''}
    assert json_content['award_procurement'] == {'status': 'finished', 'has_errors': True, 'has_warnings': False,
                                                 'message': ''}
    assert json_content['executive_compensation'] == {'status': 'failed', 'has_errors': True, 'has_warnings': False,
                                                      'message': 'test message'}
    assert json_content['sub_award'] == {'status': 'ready', 'has_errors': False, 'has_warnings': False, 'message': ''}
    assert json_content['cross'] == {'status': 'ready', 'has_errors': False, 'has_warnings': False, 'message': ''}

    # Get just one status (ignore case)
    json_response = fileHandler.get_status(sub, 'awArd')
    assert json_response.status_code == 200
    json_content = json.loads(json_response.get_data().decode('UTF-8'))
    assert len(json_content) == 1
    assert json_content['award'] == {'status': 'uploading', 'has_errors': False, 'has_warnings': False, 'message': ''}


def test_process_job_status():
    """ Tests the helper function that parses the job status of the current job for check_status """

    response_content = {'status': 'ready', 'has_errors': False, 'has_warnings': False, 'message': ''}
    job_1 = {'job_type': JOB_TYPE_DICT['file_upload'], 'job_status': JOB_STATUS_DICT['waiting'], 'error_message': ''}
    job_2 = {'job_type': JOB_TYPE_DICT['csv_record_validation'], 'job_status': JOB_STATUS_DICT['ready'],
             'error_message': ''}

    # both jobs waiting or ready
    resp = fileHandler.process_job_status([job_1, job_2], response_content)
    assert resp['status'] == 'ready'
    assert resp['has_errors'] is False
    assert resp['has_warnings'] is False
    assert resp['message'] == ''

    response_content = {'status': 'ready', 'has_errors': False, 'has_warnings': False, 'message': ''}
    # no upload job because it's cross-file
    resp = fileHandler.process_job_status([job_2], response_content)
    assert resp['status'] == 'ready'
    assert resp['has_errors'] is False
    assert resp['has_warnings'] is False
    assert resp['message'] == ''

    response_content = {'status': 'ready', 'has_errors': False, 'has_warnings': False, 'message': ''}
    job_1['job_status'] = JOB_STATUS_DICT['invalid']
    job_1['error_message'] = 'I broke'
    # one job failed
    resp = fileHandler.process_job_status([job_1, job_2], response_content)
    assert resp['status'] == 'failed'
    assert resp['has_errors'] is True
    assert resp['has_warnings'] is False
    assert resp['message'] == 'I broke'

    response_content = {'status': 'ready', 'has_errors': False, 'has_warnings': False, 'message': ''}
    job_1['error_message'] = ''
    job_1['job_status'] = JOB_STATUS_DICT['finished']
    job_2['job_status'] = JOB_STATUS_DICT['invalid']
    # validation job invalid
    resp = fileHandler.process_job_status([job_1, job_2], response_content)
    assert resp['status'] == 'finished'
    assert resp['has_errors'] is True
    assert resp['has_warnings'] is False
    assert resp['message'] == ''

    response_content = {'status': 'ready', 'has_errors': False, 'has_warnings': False, 'message': ''}
    job_1['job_status'] = JOB_STATUS_DICT['running']
    job_2['job_status'] = JOB_STATUS_DICT['ready']
    # uploading
    resp = fileHandler.process_job_status([job_1, job_2], response_content)
    assert resp['status'] == 'uploading'
    assert resp['has_errors'] is False
    assert resp['has_warnings'] is False
    assert resp['message'] == ''

    response_content = {'status': 'ready', 'has_errors': False, 'has_warnings': False, 'message': ''}
    job_1['job_status'] = JOB_STATUS_DICT['finished']
    job_2['job_status'] = JOB_STATUS_DICT['running']
    # validating
    resp = fileHandler.process_job_status([job_1, job_2], response_content)
    assert resp['status'] == 'running'
    assert resp['has_errors'] is False
    assert resp['has_warnings'] is False
    assert resp['message'] == ''

    response_content = {'status': 'ready', 'has_errors': False, 'has_warnings': False, 'message': ''}
    job_2['job_status'] = JOB_STATUS_DICT['finished']
    job_2['errors'] = 0
    job_2['warnings'] = 4
    # jobs done, has warnings
    resp = fileHandler.process_job_status([job_1, job_2], response_content)
    assert resp['status'] == 'finished'
    assert resp['has_errors'] is False
    assert resp['has_warnings'] is True
    assert resp['message'] == ''
