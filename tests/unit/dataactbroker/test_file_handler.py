from datetime import date, datetime, timedelta
import json
import os.path
from unittest.mock import Mock, MagicMock

import pytest

import calendar

from dataactbroker.handlers import fileHandler
from dataactcore.models.jobModels import JobStatus, JobType, FileType, CertifiedFilesHistory
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT
from dataactcore.utils.responseException import ResponseException
from tests.unit.dataactbroker.utils import add_models, delete_models
from tests.unit.dataactcore.factories.domain import CGACFactory
from tests.unit.dataactcore.factories.job import (JobFactory, SubmissionFactory, CertifyHistoryFactory,
                                                  SubmissionNarrativeFactory, CertifiedFilesHistoryFactory)
from tests.unit.dataactcore.factories.user import UserFactory


def list_submissions_result(d2_submission=False):
    json_response = fileHandler.list_submissions(1, 10, "mixed", d2_submission=d2_submission)
    assert json_response.status_code == 200
    return json.loads(json_response.get_data().decode('UTF-8'))


def list_submissions_sort(category, order):
    json_response = fileHandler.list_submissions(1, 10, "mixed", category, order)
    assert json_response.status_code == 200
    return json.loads(json_response.get_data().decode('UTF-8'))


@pytest.mark.usefixtures("job_constants")
def test_list_submissions_sort_success(database, monkeypatch):
    user1 = UserFactory(user_id=1, name='Oliver Queen', website_admin=True)
    user2 = UserFactory(user_id=2, name='Barry Allen')
    sub1 = SubmissionFactory(user_id=1, submission_id=1, number_of_warnings=1, reporting_start_date=date(2010, 1, 1),
                             publish_status_id=1)
    sub2 = SubmissionFactory(user_id=1, submission_id=2, number_of_warnings=1, reporting_start_date=date(2010, 1, 2),
                             publish_status_id=1)
    sub3 = SubmissionFactory(user_id=2, submission_id=3, number_of_warnings=1, reporting_start_date=date(2010, 1, 3),
                             publish_status_id=1)
    sub4 = SubmissionFactory(user_id=2, submission_id=4, number_of_warnings=1, reporting_start_date=date(2010, 1, 4),
                             publish_status_id=1)
    sub5 = SubmissionFactory(user_id=2, submission_id=5, number_of_warnings=1, reporting_start_date=date(2010, 1, 5),
                             publish_status_id=1)
    add_models(database, [user1, user2, sub1, sub2, sub3, sub4, sub5])

    monkeypatch.setattr(fileHandler, 'g', Mock(user=user1))
    result = list_submissions_sort('reporting', 'desc')
    assert result['total'] == 5
    sub = result['submissions'][0]
    index = 0
    for subit in result['submissions']:
        index += 1
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
        assert subit['user']['name'] >= sub['user']['name']
        sub = subit

    result = list_submissions_sort('submitted_by', 'desc')
    assert result['total'] == 5
    sub = result['submissions'][0]
    for subit in result['submissions']:
        assert subit['user']['name'] <= sub['user']['name']
        sub = subit
    delete_models(database, [user1, user2, sub1, sub2, sub3, sub4, sub5])


@pytest.mark.usefixtures("job_constants")
def test_list_submissions_success(database, monkeypatch):
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, number_of_warnings=1, publish_status_id=1)
    add_models(database, [user, sub])

    monkeypatch.setattr(fileHandler, 'g', Mock(user=user))
    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == "validation_successful_warnings"
    delete_models(database, [user, sub])

    sess = database.session
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=1)
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
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=1)
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
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=1)
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
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=1)
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='ready').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == "ready"
    delete_models(database, [user, sub, job])

    sess = database.session
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=1, d2_submission=True)
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='ready').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    result = list_submissions_result(d2_submission=True)
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == "ready"
    delete_models(database, [user, sub, job])


@pytest.mark.usefixtures("job_constants")
def test_list_submissions_failure(database, monkeypatch):
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, number_of_errors=1, publish_status_id=1)
    add_models(database, [user, sub])

    monkeypatch.setattr(fileHandler, 'g', Mock(user=user))
    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == "validation_errors"
    delete_models(database, [user, sub])

    sess = database.session
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=1)
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
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=1)
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='invalid').one(),
                     job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                     file_type=sess.query(FileType).filter_by(name='award').one())
    add_models(database, [user, sub, job])

    result = list_submissions_result()
    assert result['total'] == 1
    assert result['submissions'][0]['status'] == "file_errors"
    delete_models(database, [user, sub, job])


@pytest.mark.usefixtures("job_constants")
def test_list_submissions_detached(database, monkeypatch):
    user = UserFactory(user_id=1)
    sub = SubmissionFactory(user_id=1, submission_id=1, publish_status_id=1)
    d2_sub = SubmissionFactory(user_id=1, submission_id=2, d2_submission=True, publish_status_id=1)
    add_models(database, [user, sub, d2_sub])

    monkeypatch.setattr(fileHandler, 'g', Mock(user=user))
    result = list_submissions_result()
    d2_result = list_submissions_result(d2_submission=True)

    assert result['total'] == 1
    assert result['submissions'][0]['submission_id'] == sub.submission_id
    assert d2_result['total'] == 1
    assert d2_result['submissions'][0]['submission_id'] == d2_sub.submission_id
    delete_models(database, [user, sub, d2_sub])


@pytest.mark.usefixtures('user_constants')
@pytest.mark.usefixtures("job_constants")
def test_list_submissions_permissions(database, monkeypatch):
    """Verify that the user must be in the same CGAC group, the submission's
    owner, or website admin to see the submission"""
    cgac1, cgac2 = CGACFactory(), CGACFactory()
    user1, user2 = UserFactory.with_cgacs(cgac1), UserFactory()
    database.session.add_all([cgac1, cgac2, user1, user2])
    database.session.commit()
    sub = SubmissionFactory(user_id=user2.user_id, cgac_code=cgac2.cgac_code, publish_status_id=1)
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


@pytest.mark.usefixtures("job_constants")
def test_narratives(database):
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
        'FABS': ''
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
        'FABS': ''
    }

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


@pytest.mark.usefixtures("job_constants")
def test_submission_to_dict_for_status(database):
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
    monkeypatch.setattr(fileHandler, 'S3Handler', s3_url_handler)
    json_response = fileHandler.submission_report_url(
        SubmissionFactory(submission_id=2), False, 'some_file', None)
    url = json.loads(json_response.get_data().decode('utf-8'))['url']
    assert url == 'some/url/here.csv'
    assert s3_url_handler.return_value.get_signed_url.call_args == (
        ('errors', 'submission_2_some_file_error_report.csv'),
        {'method': 'GET'}
    )


def test_build_file_map_string(monkeypatch):
    monkeypatch.setattr(fileHandler, 'CONFIG_BROKER', {'local': False})
    upload_files = []
    response_dict = {}
    file_type_list = ["fabs"]
    file_dict = {"fabs":"fabs_file.csv"}
    def side_effect(filename):
        return "123_"+filename
    s3_url_handler = Mock()
    s3_url_handler.get_timestamped_filename = Mock(side_effect=side_effect)
    monkeypatch.setattr(fileHandler, 'S3Handler', s3_url_handler)
    submission = SubmissionFactory(submission_id=3)
    fh = fileHandler.FileHandler({})
    fh.build_file_map(file_dict, file_type_list, response_dict, upload_files, submission)
    for file in upload_files:
        assert file.upload_name == "3/123_"+file.file_name


@pytest.mark.usefixtures("job_constants")
def test_get_upload_file_url_local(database, monkeypatch, tmpdir):
    """ Test getting the url of the uploaded file locally. """
    file_path = str(tmpdir) + os.path.sep
    monkeypatch.setattr(fileHandler, 'CONFIG_BROKER', {'local': True, 'broker_files': file_path})

    # create and insert submission/job
    sess = database.session
    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                     job_type=sess.query(JobType).filter_by(name='file_upload').one(),
                     file_type=sess.query(FileType).filter_by(name='appropriations').one(),
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


@pytest.mark.usefixtures("job_constants")
def test_get_upload_file_url_no_file(database):
    """ Test that a proper error is thrown when an upload job doesn't have a file associated with it
        get_upload_file_url.
    """
    sess = database.session
    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                     job_type=sess.query(JobType).filter_by(name='file_upload').one(),
                     file_type=sess.query(FileType).filter_by(name='appropriations').one(),
                     filename=None)
    add_models(database, [sub, job])

    json_response = fileHandler.get_upload_file_url(sub, 'A')
    assert json_response.status_code == 400
    response = json.loads(json_response.get_data().decode('utf-8'))
    assert response['message'] == 'No file uploaded or generated for this type'


@pytest.mark.usefixtures("job_constants")
def test_get_upload_file_url_s3(database, monkeypatch):
    """ Test getting the url of the uploaded file non-locally. """
    monkeypatch.setattr(fileHandler, 'CONFIG_BROKER', {'local': False})
    s3_url_handler = Mock()
    s3_url_handler.return_value.get_signed_url.return_value = 'some/url/here.csv'
    monkeypatch.setattr(fileHandler, 'S3Handler', s3_url_handler)

    # create and insert submission/job
    sess = database.session
    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    job = JobFactory(submission_id=1, job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                     job_type=sess.query(JobType).filter_by(name='file_upload').one(),
                     file_type=sess.query(FileType).filter_by(name='appropriations').one(),
                     filename='1/some_file.csv')
    add_models(database, [sub, job])

    json_response = fileHandler.get_upload_file_url(sub, 'A')
    url = json.loads(json_response.get_data().decode('utf-8'))['url']
    assert url == 'some/url/here.csv'
    assert s3_url_handler.return_value.get_signed_url.call_args == (
        ('1', 'some_file.csv'),
        {'method': 'GET'}
    )


@pytest.mark.usefixtures("job_constants")
def test_move_certified_files(database, monkeypatch):
    # set up cgac and submission
    cgac = CGACFactory(cgac_code='zyxwv', agency_name='Test')
    sub = SubmissionFactory(cgac_code='zyxwv', number_of_errors=0, publish_status_id=1,
                            reporting_fiscal_year=2017, reporting_fiscal_period=6)
    database.session.add_all([cgac, sub])
    database.session.commit()

    # set up certify history and jobs based on submission
    sess = database.session
    cert_hist_local = CertifyHistoryFactory(submission_id=sub.submission_id)
    cert_hist_remote = CertifyHistoryFactory(submission_id=sub.submission_id)

    finished_job = sess.query(JobStatus).filter_by(name='finished').one()
    upload_job = sess.query(JobType).filter_by(name='file_upload').one()
    appropriations_job = JobFactory(submission=sub, filename="/path/to/appropriations/file_a.csv",
                                    file_type=sess.query(FileType).filter_by(name='appropriations').one(),
                                    job_type=upload_job, job_status=finished_job)
    prog_act_job = JobFactory(submission=sub, filename="/path/to/prog/act/file_b.csv",
                              file_type=sess.query(FileType).filter_by(name='program_activity').one(),
                              job_type=upload_job, job_status=finished_job)
    award_fin_job = JobFactory(submission=sub, filename="/path/to/award/fin/file_c.csv",
                               file_type=sess.query(FileType).filter_by(name='award_financial').one(),
                               job_type=upload_job, job_status=finished_job)
    award_proc_job = JobFactory(submission=sub, filename="/path/to/award/proc/file_d1.csv",
                                file_type=sess.query(FileType).filter_by(name='award_procurement').one(),
                                job_type=upload_job, job_status=finished_job)
    award_job = JobFactory(submission=sub, filename="/path/to/award/file_d2.csv",
                           file_type=sess.query(FileType).filter_by(name='award').one(),
                           job_type=upload_job, job_status=finished_job)
    exec_comp_job = JobFactory(submission=sub, filename="/path/to/exec/comp/file_e.csv",
                               file_type=sess.query(FileType).filter_by(name='executive_compensation').one(),
                               job_type=upload_job, job_status=finished_job)
    sub_award_job = JobFactory(submission=sub, filename="/path/to/sub/award/file_f.csv",
                               file_type=sess.query(FileType).filter_by(name='sub_award').one(),
                               job_type=upload_job, job_status=finished_job)

    award_fin_narr = SubmissionNarrativeFactory(submission=sub, narrative="Test narrative",
                                                file_type=sess.query(FileType).filter_by(name='award_financial').one())
    database.session.add_all([cert_hist_local, cert_hist_remote, appropriations_job, prog_act_job, award_fin_job,
                              award_proc_job, award_job, exec_comp_job, sub_award_job, award_fin_narr])
    database.session.commit()

    s3_url_handler = Mock()
    monkeypatch.setattr(fileHandler, 'S3Handler', s3_url_handler)
    monkeypatch.setattr(fileHandler, 'CONFIG_BROKER', {'aws_bucket': 'original_bucket',
                                                       'certified_bucket': 'cert_bucket'})
    monkeypatch.setattr(fileHandler, 'CONFIG_SERVICES', {'error_report_path': '/path/to/error/reports/'})

    fh = fileHandler.FileHandler(Mock())

    # test local certification
    fh.move_certified_files(sub, cert_hist_local, True)
    local_id = cert_hist_local.certify_history_id

    # make sure we have the right number of history entries
    all_local_certs = sess.query(CertifiedFilesHistory).filter_by(certify_history_id=local_id).all()
    assert len(all_local_certs) == 11

    c_cert_hist = sess.query(CertifiedFilesHistory).\
        filter_by(certify_history_id=local_id,
                  file_type_id=sess.query(FileType).filter_by(name='award_financial').one().file_type_id).one()
    assert c_cert_hist.filename == "/path/to/award/fin/file_c.csv"
    assert c_cert_hist.warning_filename == "/path/to/error/reports/submission_{}_award_financial_warning_report.csv".\
        format(sub.submission_id)
    assert c_cert_hist.narrative == "Test narrative"

    # cross-file warnings
    warning_cert_hist = sess.query(CertifiedFilesHistory).filter_by(certify_history_id=local_id, file_type=None).all()
    assert len(warning_cert_hist) == 4
    assert warning_cert_hist[0].narrative is None

    warning_cert_hist_files = [hist.warning_filename for hist in warning_cert_hist]
    assert "/path/to/error/reports/submission_{}_cross_warning_appropriations_program_activity.csv".\
        format(sub.submission_id) in warning_cert_hist_files

    # test remote certification
    fh.move_certified_files(sub, cert_hist_remote, False)
    remote_id = cert_hist_remote.certify_history_id

    c_cert_hist = sess.query(CertifiedFilesHistory). \
        filter_by(certify_history_id=remote_id,
                  file_type_id=sess.query(FileType).filter_by(name='award_financial').one().file_type_id).one()
    assert c_cert_hist.filename == "zyxwv/2017/2/{}/file_c.csv".format(remote_id)
    assert c_cert_hist.warning_filename == "zyxwv/2017/2/{}/submission_{}_award_financial_warning_report.csv". \
        format(remote_id, sub.submission_id)


@pytest.mark.usefixtures("job_constants")
def test_list_certifications(database):
    # set up submission
    sub = SubmissionFactory()
    database.session.add(sub)
    database.session.commit()

    # set up certify history, make sure the empty one comes last in the list
    cert_hist_empty = CertifyHistoryFactory(submission=sub, created_at=datetime.utcnow() - timedelta(days=1))
    cert_hist = CertifyHistoryFactory(submission=sub)
    database.session.add_all([cert_hist_empty, cert_hist])
    database.session.commit()

    # add some data to certified_files_history for the cert_history ID
    sess = database.session
    history_id = cert_hist.certify_history_id
    sub_id = sub.submission_id
    file_hist_1 = CertifiedFilesHistoryFactory(certify_history_id=history_id, submission_id=sub_id,
                                               filename="/path/to/file_a.csv",
                                               warning_filename="/path/to/warning_file_a.csv",
                                               narrative="A has a narrative",
                                               file_type=sess.query(FileType).filter_by(name='appropriations').one())
    file_hist_2 = CertifiedFilesHistoryFactory(certify_history_id=history_id, submission_id=sub_id,
                                               filename="/path/to/file_d2.csv",
                                               warning_filename=None,
                                               file_type=sess.query(FileType).filter_by(name='award').one())
    file_hist_3 = CertifiedFilesHistoryFactory(certify_history_id=history_id, submission_id=sub_id,
                                               filename=None,
                                               warning_filename="/path/to/warning_file_cross_test.csv",
                                               file_type=None)
    database.session.add_all([file_hist_1, file_hist_2, file_hist_3])
    database.session.commit()

    json_response = fileHandler.list_certifications(sub)
    response_dict = json.loads(json_response.get_data().decode('utf-8'))
    assert len(response_dict["certifications"]) == 2

    has_file_list = response_dict["certifications"][0]
    empty_file_list = response_dict["certifications"][1]

    # asserts for certification with files associated
    assert len(has_file_list["certified_files"]) == 4
    assert has_file_list["certified_files"][0]["is_warning"] is False
    assert has_file_list["certified_files"][0]["filename"] == "file_a.csv"
    assert has_file_list["certified_files"][0]["narrative"] == "A has a narrative"

    assert has_file_list["certified_files"][1]["is_warning"]
    assert has_file_list["certified_files"][1]["narrative"] is None

    # asserts for certification without files associated
    assert len(empty_file_list["certified_files"]) == 0


def test_file_history_url(database, monkeypatch):
    sub = SubmissionFactory()
    database.session.add(sub)
    database.session.commit()

    # set up certify history so it works
    cert_hist = CertifyHistoryFactory(submission=sub)
    database.session.add(cert_hist)
    database.session.commit()

    file_hist = CertifiedFilesHistoryFactory(certify_history_id=cert_hist.certify_history_id,
                                             submission_id=sub.submission_id, filename="/path/to/file_d2.csv",
                                             warning_filename="/path/to/warning_file_cross.csv",
                                             narrative=None, file_type=None)
    database.session.add(file_hist)
    database.session.commit()

    s3_url_handler = Mock()
    s3_url_handler.return_value.get_signed_url.return_value = 'some/url/here.csv'
    monkeypatch.setattr(fileHandler, 'S3Handler', s3_url_handler)

    # checking for local response to non-warning file
    json_response = fileHandler.file_history_url(sub, file_hist.certified_files_history_id, False, True)
    url = json.loads(json_response.get_data().decode('utf-8'))["url"]
    assert url == "/path/to/file_d2.csv"

    # local response to warning file
    json_response = fileHandler.file_history_url(sub, file_hist.certified_files_history_id, True, True)
    url = json.loads(json_response.get_data().decode('utf-8'))["url"]
    assert url == "/path/to/warning_file_cross.csv"

    # generic test to make sure it's reaching the s3 handler properly
    json_response = fileHandler.file_history_url(sub, file_hist.certified_files_history_id, False, False)
    url = json.loads(json_response.get_data().decode('utf-8'))["url"]
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


@pytest.mark.usefixtures("job_constants")
def test_get_status_fabs(database):
    """ Test get status function for a fabs submission """
    sess = database.session

    sub = SubmissionFactory(submission_id=1, d2_submission=True)
    job_up = JobFactory(submission_id=sub.submission_id,
                        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
                        file_type=sess.query(FileType).filter_by(name='fabs').one(),
                        job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                        number_of_errors=0, number_of_warnings=0)
    job_val = JobFactory(submission_id=sub.submission_id,
                         job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                         file_type=sess.query(FileType).filter_by(name='fabs').one(),
                         job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                         number_of_errors=0, number_of_warnings=4)

    sess.add_all([sub, job_up, job_val])
    sess.commit()

    json_response = fileHandler.get_status(sub)
    assert json_response.status_code == 200
    json_content = json.loads(json_response.get_data().decode('UTF-8'))
    assert json_content['fabs'] == {'status': 'finished', 'has_errors': False, 'has_warnings': True, 'message': ''}


@pytest.mark.usefixtures("job_constants")
def test_get_status_dabs(database):
    """ Test get status function for a dabs submission, including all possible statuses and case insensitivity """
    sess = database.session

    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    upload_job = sess.query(JobType).filter_by(name='file_upload').one()
    validation_job = sess.query(JobType).filter_by(name='csv_record_validation').one()
    finished_status = sess.query(JobStatus).filter_by(name='finished').one()

    # Completed, warnings, errors
    job_1_up = JobFactory(submission_id=sub.submission_id, job_type=upload_job,
                          file_type=sess.query(FileType).filter_by(name='appropriations').one(),
                          job_status=finished_status, number_of_errors=0, number_of_warnings=0, error_message=None)
    job_1_val = JobFactory(submission_id=sub.submission_id, job_type=validation_job,
                           file_type=sess.query(FileType).filter_by(name='appropriations').one(),
                           job_status=finished_status, number_of_errors=10, number_of_warnings=4, error_message=None)
    # Invalid upload
    job_2_up = JobFactory(submission_id=sub.submission_id, job_type=upload_job,
                          file_type=sess.query(FileType).filter_by(name='program_activity').one(),
                          job_status=sess.query(JobStatus).filter_by(name='invalid').one(),
                          number_of_errors=0, number_of_warnings=0, error_message=None)
    job_2_val = JobFactory(submission_id=sub.submission_id, job_type=validation_job,
                           file_type=sess.query(FileType).filter_by(name='program_activity').one(),
                           job_status=sess.query(JobStatus).filter_by(name='waiting').one(),
                           number_of_errors=0, number_of_warnings=0, error_message=None)
    # Validating
    job_3_up = JobFactory(submission_id=sub.submission_id, job_type=upload_job,
                          file_type=sess.query(FileType).filter_by(name='award_financial').one(),
                          job_status=finished_status, number_of_errors=0, number_of_warnings=0, error_message=None)
    job_3_val = JobFactory(submission_id=sub.submission_id, job_type=validation_job,
                           file_type=sess.query(FileType).filter_by(name='award_financial').one(),
                           job_status=sess.query(JobStatus).filter_by(name='running').one(),
                           number_of_errors=0, number_of_warnings=0, error_message=None)
    # Uploading
    job_4_up = JobFactory(submission_id=sub.submission_id, job_type=upload_job,
                          file_type=sess.query(FileType).filter_by(name='award').one(),
                          job_status=sess.query(JobStatus).filter_by(name='running').one(),
                          number_of_errors=0, number_of_warnings=0, error_message=None)
    job_4_val = JobFactory(submission_id=sub.submission_id, job_type=validation_job,
                           file_type=sess.query(FileType).filter_by(name='award').one(),
                           job_status=sess.query(JobStatus).filter_by(name='ready').one(),
                           number_of_errors=0, number_of_warnings=0, error_message=None)
    # Invalid on validation
    job_5_up = JobFactory(submission_id=sub.submission_id, job_type=upload_job,
                          file_type=sess.query(FileType).filter_by(name='award_procurement').one(),
                          job_status=finished_status, number_of_errors=0, number_of_warnings=0, error_message=None)
    job_5_val = JobFactory(submission_id=sub.submission_id, job_type=validation_job,
                           file_type=sess.query(FileType).filter_by(name='award_procurement').one(),
                           job_status=sess.query(JobStatus).filter_by(name='invalid').one(),
                           number_of_errors=0, number_of_warnings=0, error_message=None)
    # Failed
    job_6_up = JobFactory(submission_id=sub.submission_id, job_type=upload_job,
                          file_type=sess.query(FileType).filter_by(name='executive_compensation').one(),
                          job_status=sess.query(JobStatus).filter_by(name='failed').one(),
                          number_of_errors=0, number_of_warnings=0, error_message='test message')
    # Ready
    job_7_up = JobFactory(submission_id=sub.submission_id, job_type=upload_job,
                          file_type=sess.query(FileType).filter_by(name='sub_award').one(),
                          job_status=sess.query(JobStatus).filter_by(name='ready').one(),
                          number_of_errors=0, number_of_warnings=0, error_message=None)
    # Waiting
    job_8_val = JobFactory(submission_id=sub.submission_id,
                           job_type=sess.query(JobType).filter_by(name='validation').one(),
                           file_type=None,
                           job_status=sess.query(JobStatus).filter_by(name='waiting').one(),
                           number_of_errors=0, number_of_warnings=5, error_message=None)

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


@pytest.mark.usefixtures("job_constants")
def test_check_generation_prereqs_ef_valid(database):
    """ Tests a set of conditions that passes the prerequisite checks to allow E/F files to be generated. Show that
        warnings do not prevent generation.
    """
    sess = database.session

    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    cross_val = JobFactory(submission_id=sub.submission_id,
                           job_type=sess.query(JobType).filter_by(name='validation').one(),
                           file_type=None,
                           job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                           number_of_errors=0, number_of_warnings=1, error_message=None)
    sess.add_all([sub, cross_val])
    sess.commit()

    can_generate = fileHandler.check_generation_prereqs(sub.submission_id, 'E')
    assert can_generate is True


@pytest.mark.usefixtures("job_constants")
def test_check_generation_prereqs_ef_not_finished(database):
    """ Tests a set of conditions that has cross-file still waiting, fail the generation check for E/F files. """
    sess = database.session

    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    cross_val = JobFactory(submission_id=sub.submission_id,
                           job_type=sess.query(JobType).filter_by(name='validation').one(),
                           file_type=None,
                           job_status=sess.query(JobStatus).filter_by(name='waiting').one(),
                           number_of_errors=0, number_of_warnings=0, error_message=None)
    sess.add_all([sub, cross_val])
    sess.commit()

    can_generate = fileHandler.check_generation_prereqs(sub.submission_id, 'E')
    assert can_generate is False


@pytest.mark.usefixtures("job_constants")
def test_check_generation_prereqs_ef_has_errors(database):
    """ Tests a set of conditions that has an error in cross-file, fail the generation check for E/F files. """
    sess = database.session

    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    cross_val = JobFactory(submission_id=sub.submission_id,
                           job_type=sess.query(JobType).filter_by(name='validation').one(),
                           file_type=None,
                           job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                           number_of_errors=1, number_of_warnings=0, error_message=None)
    sess.add_all([sub, cross_val])
    sess.commit()

    can_generate = fileHandler.check_generation_prereqs(sub.submission_id, 'E')
    assert can_generate is False


@pytest.mark.usefixtures("job_constants")
def test_check_generation_prereqs_d_valid(database):
    """ Tests a set of conditions that passes the prerequisite checks to allow D files to be generated. Show that
        warnings do not prevent generation.
    """
    sess = database.session

    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    job_1 = JobFactory(submission_id=sub.submission_id,
                       job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                       file_type=sess.query(FileType).filter_by(name='appropriations').one(),
                       job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                       number_of_errors=0, number_of_warnings=0, error_message=None)
    job_2 = JobFactory(submission_id=sub.submission_id,
                       job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                       file_type=sess.query(FileType).filter_by(name='program_activity').one(),
                       job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                       number_of_errors=0, number_of_warnings=0, error_message=None)
    job_3 = JobFactory(submission_id=sub.submission_id,
                       job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                       file_type=sess.query(FileType).filter_by(name='award_financial').one(),
                       job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                       number_of_errors=0, number_of_warnings=1, error_message=None)
    sess.add_all([sub, job_1, job_2, job_3])
    sess.commit()

    can_generate = fileHandler.check_generation_prereqs(sub.submission_id, 'D1')
    assert can_generate is True


@pytest.mark.usefixtures("job_constants")
def test_check_generation_prereqs_d_not_finished(database):
    """ Tests a set of conditions that has one of the A,B,C files incomplete, prevent D file generation. """
    sess = database.session

    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    job_1 = JobFactory(submission_id=sub.submission_id,
                       job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                       file_type=sess.query(FileType).filter_by(name='appropriations').one(),
                       job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                       number_of_errors=0, number_of_warnings=0, error_message=None)
    job_2 = JobFactory(submission_id=sub.submission_id,
                       job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                       file_type=sess.query(FileType).filter_by(name='program_activity').one(),
                       job_status=sess.query(JobStatus).filter_by(name='waiting').one(),
                       number_of_errors=0, number_of_warnings=0, error_message=None)
    job_3 = JobFactory(submission_id=sub.submission_id,
                       job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                       file_type=sess.query(FileType).filter_by(name='award_financial').one(),
                       job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                       number_of_errors=0, number_of_warnings=0, error_message=None)
    sess.add_all([sub, job_1, job_2, job_3])
    sess.commit()

    can_generate = fileHandler.check_generation_prereqs(sub.submission_id, 'D1')
    assert can_generate is False


@pytest.mark.usefixtures("job_constants")
def test_check_generation_prereqs_d_has_errors(database):
    """ Tests a set of conditions that has an error in one of the A,B,C files, prevent D file generation. """
    sess = database.session

    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    job_1 = JobFactory(submission_id=sub.submission_id,
                       job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                       file_type=sess.query(FileType).filter_by(name='appropriations').one(),
                       job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                       number_of_errors=1, number_of_warnings=0, error_message=None)
    job_2 = JobFactory(submission_id=sub.submission_id,
                       job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                       file_type=sess.query(FileType).filter_by(name='program_activity').one(),
                       job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                       number_of_errors=0, number_of_warnings=0, error_message=None)
    job_3 = JobFactory(submission_id=sub.submission_id,
                       job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
                       file_type=sess.query(FileType).filter_by(name='award_financial').one(),
                       job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                       number_of_errors=0, number_of_warnings=0, error_message=None)
    sess.add_all([sub, job_1, job_2, job_3])
    sess.commit()

    can_generate = fileHandler.check_generation_prereqs(sub.submission_id, 'D1')
    assert can_generate is False


@pytest.mark.usefixtures("job_constants")
def test_check_generation_prereqs_bad_type(database):
    """ Tests that check_generation_prereqs raises an error if an invalid type is provided. """
    sess = database.session
    sub = SubmissionFactory()
    sess.add(sub)
    sess.commit()

    with pytest.raises(ResponseException):
        fileHandler.check_generation_prereqs(sub.submission_id, 'A')
