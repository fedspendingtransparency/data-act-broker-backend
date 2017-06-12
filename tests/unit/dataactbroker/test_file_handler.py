from datetime import date, datetime, timedelta
import json
import os.path
from unittest.mock import Mock

import pytest

import calendar

from dataactbroker.handlers import fileHandler
from dataactcore.models.jobModels import JobStatus, JobType, FileType, CertifiedFilesHistory
from dataactcore.utils.responseException import ResponseException
from tests.unit.dataactbroker.utils import add_models, delete_models
from tests.unit.dataactcore.factories.domain import CGACFactory
from tests.unit.dataactcore.factories.job import (JobFactory, SubmissionFactory, CertifyHistoryFactory,
                                                  SubmissionNarrativeFactory, CertifiedFilesHistoryFactory)
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
        assert subit['user']['name'] >= sub['user']['name']
        sub = subit

    result = list_submissions_sort('submitted_by', 'desc')
    assert result['total'] == 5
    sub = result['submissions'][0]
    for subit in result['submissions']:
        assert subit['user']['name'] <= sub['user']['name']
        sub = subit
    delete_models(database, [user1, user2, sub1, sub2, sub3, sub4, sub5])


def test_list_submissions_success(database, job_constants, monkeypatch):
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


def test_list_submissions_failure(database, job_constants, monkeypatch):
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


@pytest.mark.usefixtures('user_constants')
def test_list_submissions_permissions(database, monkeypatch, job_constants):
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
    monkeypatch.setattr(fileHandler, 'S3Handler', s3_url_handler)
    json_response = fileHandler.submission_report_url(
        SubmissionFactory(submission_id=2), False, 'some_file', None)
    url = json.loads(json_response.get_data().decode('utf-8'))['url']
    assert url == 'some/url/here.csv'
    assert s3_url_handler.return_value.get_signed_url.call_args == (
        ('errors', 'submission_2_some_file_error_report.csv'),
        {'method': 'GET'}
    )


def test_move_certified_files(database, monkeypatch, job_constants):
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


def test_list_certifications(database, job_constants):
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
