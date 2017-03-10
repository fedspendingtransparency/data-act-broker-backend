from datetime import date, datetime
import json
import os.path
from unittest.mock import Mock

import pytest

from dataactbroker.handlers import fileHandler
from dataactcore.models.jobModels import JobStatus, JobType, FileType
from dataactcore.utils.responseException import ResponseException
from tests.unit.dataactbroker.utils import add_models, delete_models
from tests.unit.dataactcore.factories.domain import CGACFactory
from tests.unit.dataactcore.factories.job import JobFactory, SubmissionFactory
from tests.unit.dataactcore.factories.user import UserFactory


def list_submissions_result():
    json_response = fileHandler.list_submissions(1, 10, "mixed")
    assert json_response.status_code == 200
    return json.loads(json_response.get_data().decode('UTF-8'))


def list_submissions_sort(category, order):
    json_response = fileHandler.list_submissions(1, 10, "mixed", category, order)
    assert json_response.status_code == 200
    return json.loads(json_response.get_data().decode('UTF-8'))


def test_list_submissions_sort_success(database, job_constants, monkeypatch):
    user1 = UserFactory(user_id=1, name='Oliver Queen', website_admin=True)
    user2 = UserFactory(user_id=2, name='Barry Allen')
    sub1 = SubmissionFactory(user_id=1, submission_id=1, number_of_warnings=1, reporting_start_date=date(2010, 1, 1))
    sub2 = SubmissionFactory(user_id=1, submission_id=2, number_of_warnings=1, reporting_start_date=date(2010, 1, 2))
    sub3 = SubmissionFactory(user_id=2, submission_id=3, number_of_warnings=1, reporting_start_date=date(2010, 1, 3))
    sub4 = SubmissionFactory(user_id=2, submission_id=4, number_of_warnings=1, reporting_start_date=date(2010, 1, 4))
    sub5 = SubmissionFactory(user_id=2, submission_id=5, number_of_warnings=1, reporting_start_date=date(2010, 1, 5))
    add_models(database, [user1, user2, sub1, sub2, sub3, sub4, sub5])

    monkeypatch.setattr(fileHandler, 'g', Mock(user=user1))
    result = list_submissions_sort('reporting', 'desc')
    assert result['total'] == 5
    sub = result['submissions'][0]
    for subit in result['submissions']:
        assert subit['reporting_start_date'] <= sub['reporting_start_date']
        sub = subit

    result = list_submissions_sort('reporting', 'asc')
    assert result['total'] == 5
    sub = result['submissions'][0]
    for subit in result['submissions']:
        assert subit['reporting_start_date'] >= sub['reporting_start_date']
        sub = subit

    result = list_submissions_sort('submitted_by', 'asc')
    assert result['total'] == 5
    sub = result['submissions'][0]
    for subit in result['submissions']:
        assert subit['user']['name'] <= sub['user']['name']
        sub = subit

    result = list_submissions_sort('submitted_by', 'desc')
    assert result['total'] == 5
    sub = result['submissions'][0]
    for subit in result['submissions']:
        assert subit['user']['name'] >= sub['user']['name']
        sub = subit
    delete_models(database, [user1, user2, sub1, sub2, sub3, sub4, sub5])


def test_list_submissions_success(database, job_constants, monkeypatch):
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, number_of_warnings=1)
    add_models(database, [user, sub])

    monkeypatch.setattr(fileHandler, 'g', Mock(user=user))
    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == "validation_successful_warnings"
    delete_models(database, [user, sub])

    sess = database.session
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1)
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == "validation_successful"
    delete_models(database, [user, sub, job])

    sess = database.session
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1)
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='running').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == "running"
    delete_models(database, [user, sub, job])

    sess = database.session
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1)
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='waiting').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == "waiting"
    delete_models(database, [user, sub, job])

    sess = database.session
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1)
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='ready').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == "ready"
    delete_models(database, [user, sub, job])


def test_list_submissions_failure(database, job_constants, monkeypatch):
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, number_of_errors=1)
    add_models(database, [user, sub])

    monkeypatch.setattr(fileHandler, 'g', Mock(user=user))
    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == "validation_errors"
    delete_models(database, [user, sub])

    sess = database.session
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1)
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='failed').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == "failed"
    delete_models(database, [user, sub, job])

    sess = database.session
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1)
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='invalid').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == "file_errors"
    delete_models(database, [user, sub, job])


@pytest.mark.usefixtures('user_constants')
def test_list_submissions_permissions(database, monkeypatch):
    """Verify that the user must be in the same CGAC group, the submission's
    owner, or website admin to see the submission"""
    cgac1, cgac2 = CGACFactory(), CGACFactory()
    user1, user2 = UserFactory.with_cgacs(cgac1), UserFactory()
    database.session.add_all([cgac1, cgac2, user1, user2])
    database.session.commit()
    sub = SubmissionFactory(user_id=user2.user_id, cgac_code=cgac2.cgac_code)
    database.session.add(sub)
    database.session.commit()

    monkeypatch.setattr(fileHandler, 'g', Mock(user=user1))
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


def test_narratives(database, job_constants):
    """Verify that we can add, retrieve, and update submission narratives. Not
    quite a unit test as it covers a few functions in sequence"""
    sub1, sub2 = SubmissionFactory(), SubmissionFactory()
    database.session.add_all([sub1, sub2])
    database.session.commit()

    # Write some narratives
    result = fileHandler.update_narratives(sub1, {'B': 'BBBBBB', 'E': 'EEEEEE'})
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
        'F': '',
        'D2_detached': ''
    }

    # Replace the narratives
    result = fileHandler.update_narratives(sub1, {'A': 'AAAAAA', 'E': 'E2E2E2'})
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
        'F': '',
        'D2_detached': ''
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
        reporting_end_date=datetime.strptime('09/2016', '%m/%Y').date())),
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


def test_submission_to_dict_for_status(database, job_constants):
    cgac = CGACFactory(cgac_code='abcdef', agency_name='Age')
    sub = SubmissionFactory(cgac_code='abcdef', number_of_errors=1234, publish_status_id=1)
    database.session.add_all([cgac, sub])
    database.session.commit()

    result = fileHandler.submission_to_dict_for_status(sub)
    assert result['cgac_code'] == 'abcdef'
    assert result['agency_name'] == 'Age'
    assert result['number_of_errors'] == 1234


def test_submission_report_url_local(monkeypatch, tmpdir):
    file_path = str(tmpdir) + os.path.sep
    monkeypatch.setattr(fileHandler, 'CONFIG_BROKER', {'local': True, 'broker_files': file_path})
    json_response = fileHandler.submission_report_url(
        SubmissionFactory(submission_id=4), True, 'some_file', 'another_file')
    url = json.loads(json_response.get_data().decode('utf-8'))['url']
    assert url == os.path.join(file_path, 'submission_4_cross_warning_some_file_another_file.csv')


def test_submission_report_url_s3(monkeypatch):
    monkeypatch.setattr(fileHandler, 'CONFIG_BROKER', {'local': False})
    s3_url_handler = Mock()
    s3_url_handler.return_value.get_signed_url.return_value = 'some/url/here.csv'
    monkeypatch.setattr(fileHandler, 'S3UrlHandler', s3_url_handler)
    json_response = fileHandler.submission_report_url(
        SubmissionFactory(submission_id=2), False, 'some_file', None)
    url = json.loads(json_response.get_data().decode('utf-8'))['url']
    assert url == 'some/url/here.csv'
    assert s3_url_handler.return_value.get_signed_url.call_args == (
        ('errors', 'submission_2_some_file_error_report.csv'),
        {'method': 'GET'}
    )
