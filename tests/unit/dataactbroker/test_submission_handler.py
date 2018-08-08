import datetime
import pytest
import json

from flask import Flask, g
from unittest.mock import Mock

from dataactbroker.handlers import fileHandler
from dataactbroker.handlers.submission_handler import (certify_dabs_submission, get_submission_metadata,
                                                       get_revalidation_threshold, get_submission_data)

from dataactcore.models.lookups import PUBLISH_STATUS_DICT, JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT
from dataactcore.models.jobModels import CertifyHistory

from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory
from tests.unit.dataactcore.factories.job import (SubmissionFactory, JobFactory, CertifyHistoryFactory,
                                                  RevalidationThresholdFactory)
from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactcore.factories.user import UserFactory


@pytest.mark.usefixtures("job_constants")
def test_get_submission_metadata_quarterly_dabs_cgac(database):
    """ Tests the get_submission_metadata function for quarterly dabs submissions """
    sess = database.session

    now = datetime.datetime.utcnow()
    now_plus_10 = now + datetime.timedelta(minutes=10)
    cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')
    frec_cgac = CGACFactory(cgac_code='999', agency_name='FREC CGAC')
    frec = FRECFactory(frec_code='0001', agency_name='FREC Agency', cgac=frec_cgac)

    sub = SubmissionFactory(submission_id=1, created_at=now, updated_at=now_plus_10, cgac_code=cgac.cgac_code,
                            reporting_fiscal_period=3, reporting_fiscal_year=2017, is_quarter_format=True,
                            publish_status_id=PUBLISH_STATUS_DICT['updated'], d2_submission=False, number_of_errors=40,
                            number_of_warnings=200)
    # Job for submission
    job = JobFactory(submission_id=sub.submission_id, last_validated=now_plus_10,
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'], job_status_id=JOB_STATUS_DICT['finished'],
                     file_type_id=FILE_TYPE_DICT['appropriations'], number_of_rows=3, file_size=7655)
    job_2 = JobFactory(submission_id=sub.submission_id, last_validated=now_plus_10,
                       job_type_id=JOB_TYPE_DICT['csv_record_validation'], job_status_id=JOB_STATUS_DICT['finished'],
                       file_type_id=FILE_TYPE_DICT['program_activity'], number_of_rows=7, file_size=12345)

    sess.add_all([cgac, frec_cgac, frec, sub, job, job_2])
    sess.commit()

    # Test for Quarterly, updated DABS cgac submission
    expected_results = {
        'cgac_code': cgac.cgac_code,
        'frec_code': None,
        'agency_name': cgac.agency_name,
        'number_of_errors': 40,
        'number_of_warnings': 200,
        'number_of_rows': 10,
        'total_size': 20000,
        'created_on': now.strftime('%m/%d/%Y'),
        'last_updated': now_plus_10.strftime("%Y-%m-%dT%H:%M:%S"),
        'last_validated': now_plus_10.strftime('%m/%d/%Y'),
        'reporting_period': 'Q1/2017',
        'publish_status': 'updated',
        'quarterly_submission': True,
        'fabs_submission': False,
        'fabs_meta': None
    }

    results = get_submission_metadata(sub)
    assert results == expected_results


@pytest.mark.usefixtures("job_constants")
def test_get_submission_metadata_quarterly_dabs_frec(database):
    """ Tests the get_submission_metadata function for quarterly dabs submissions frec """
    sess = database.session

    now = datetime.datetime.utcnow()
    frec_cgac = CGACFactory(cgac_code='999', agency_name='FREC CGAC')
    frec = FRECFactory(frec_code='0001', agency_name='FREC Agency', cgac=frec_cgac)

    sub = SubmissionFactory(submission_id=2, created_at=now, updated_at=now, cgac_code=None, frec_code=frec.frec_code,
                            reporting_fiscal_period=6, reporting_fiscal_year=2010, is_quarter_format=True,
                            publish_status_id=PUBLISH_STATUS_DICT['published'], d2_submission=False, number_of_errors=0,
                            number_of_warnings=0)

    sess.add_all([frec_cgac, frec, sub])
    sess.commit()

    expected_results = {
        'cgac_code': None,
        'frec_code': frec.frec_code,
        'agency_name': frec.agency_name,
        'number_of_errors': 0,
        'number_of_warnings': 0,
        'number_of_rows': 0,
        'total_size': 0,
        'created_on': now.strftime('%m/%d/%Y'),
        'last_updated': now.strftime("%Y-%m-%dT%H:%M:%S"),
        'last_validated': '',
        'reporting_period': 'Q2/2010',
        'publish_status': 'published',
        'quarterly_submission': True,
        'fabs_submission': False,
        'fabs_meta': None
    }

    results = get_submission_metadata(sub)
    assert results == expected_results


@pytest.mark.usefixtures("job_constants")
def test_get_submission_metadata_monthly_dabs(database):
    """ Tests the get_submission_metadata function for monthly dabs submissions """
    sess = database.session

    now = datetime.datetime.utcnow()
    now_plus_10 = now + datetime.timedelta(minutes=10)
    start_date = datetime.date(2000, 1, 1)
    cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')

    sub = SubmissionFactory(submission_id=3, created_at=now, updated_at=now_plus_10, cgac_code=cgac.cgac_code,
                            reporting_fiscal_period=4, reporting_fiscal_year=2016, is_quarter_format=False,
                            publish_status_id=PUBLISH_STATUS_DICT['unpublished'], d2_submission=False,
                            reporting_start_date=start_date, number_of_errors=20, number_of_warnings=0)

    sess.add_all([cgac, sub])
    sess.commit()

    expected_results = {
        'cgac_code': cgac.cgac_code,
        'frec_code': None,
        'agency_name': cgac.agency_name,
        'number_of_errors': 20,
        'number_of_warnings': 0,
        'number_of_rows': 0,
        'total_size': 0,
        'created_on': now.strftime('%m/%d/%Y'),
        'last_updated': now_plus_10.strftime("%Y-%m-%dT%H:%M:%S"),
        'last_validated': '',
        'reporting_period': start_date.strftime('%m/%Y'),
        'publish_status': 'unpublished',
        'quarterly_submission': False,
        'fabs_submission': False,
        'fabs_meta': None
    }

    results = get_submission_metadata(sub)
    assert results == expected_results


@pytest.mark.usefixtures("job_constants")
def test_get_submission_metadata_unpublished_fabs(database):
    """ Tests the get_submission_metadata function for unpublished fabs submissions """
    sess = database.session

    now = datetime.datetime.utcnow()
    start_date = datetime.date(2000, 1, 1)
    cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')
    frec_cgac = CGACFactory(cgac_code='999', agency_name='FREC CGAC')
    frec = FRECFactory(frec_code='0001', agency_name='FREC Agency', cgac=frec_cgac)

    sub = SubmissionFactory(submission_id=4, created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                            reporting_fiscal_period=1, reporting_fiscal_year=2015, is_quarter_format=False,
                            publish_status_id=PUBLISH_STATUS_DICT['unpublished'], d2_submission=True,
                            reporting_start_date=start_date, number_of_errors=4, number_of_warnings=1)

    sess.add_all([cgac, frec_cgac, frec, sub])
    sess.commit()

    expected_results = {
        'cgac_code': cgac.cgac_code,
        'frec_code': None,
        'agency_name': cgac.agency_name,
        'number_of_errors': 4,
        'number_of_warnings': 1,
        'number_of_rows': 0,
        'total_size': 0,
        'created_on': now.strftime('%m/%d/%Y'),
        'last_updated': now.strftime("%Y-%m-%dT%H:%M:%S"),
        'last_validated': '',
        'reporting_period': start_date.strftime('%m/%Y'),
        'publish_status': 'unpublished',
        'quarterly_submission': False,
        'fabs_submission': True,
        'fabs_meta': {'publish_date': None, 'published_file': None, 'total_rows': 0, 'valid_rows': 0}
    }

    results = get_submission_metadata(sub)
    assert results == expected_results


@pytest.mark.usefixtures("job_constants")
def test_get_submission_metadata_published_fabs(database):
    """ Tests the get_submission_metadata function for published fabs submissions """
    sess = database.session

    now = datetime.datetime.utcnow()
    now_plus_10 = now + datetime.timedelta(minutes=10)
    start_date = datetime.date(2000, 1, 1)
    cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')
    frec_cgac = CGACFactory(cgac_code='999', agency_name='FREC CGAC')
    frec = FRECFactory(frec_code='0001', agency_name='FREC Agency', cgac=frec_cgac)

    sub = SubmissionFactory(submission_id=5, created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                            reporting_fiscal_period=5, reporting_fiscal_year=2010, is_quarter_format=False,
                            publish_status_id=PUBLISH_STATUS_DICT['published'], d2_submission=True,
                            reporting_start_date=start_date, number_of_errors=0, number_of_warnings=2)
    # Data for FABS
    dafa_1 = DetachedAwardFinancialAssistanceFactory(submission_id=sub.submission_id, is_valid=True)
    dafa_2 = DetachedAwardFinancialAssistanceFactory(submission_id=sub.submission_id, is_valid=False)
    cert_hist = CertifyHistoryFactory(submission=sub, created_at=now_plus_10)

    sess.add_all([cgac, frec_cgac, frec, sub, dafa_1, dafa_2, cert_hist])
    sess.commit()

    expected_results = {
        'cgac_code': cgac.cgac_code,
        'frec_code': None,
        'agency_name': cgac.agency_name,
        'number_of_errors': 0,
        'number_of_warnings': 2,
        'number_of_rows': 0,
        'total_size': 0,
        'created_on': now.strftime('%m/%d/%Y'),
        'last_updated': now.strftime("%Y-%m-%dT%H:%M:%S"),
        'last_validated': '',
        'reporting_period': start_date.strftime('%m/%Y'),
        'publish_status': 'published',
        'quarterly_submission': False,
        'fabs_submission': True,
        'fabs_meta': {
            'publish_date': now_plus_10.strftime('%-I:%M%p %m/%d/%Y'),
            'published_file': None,
            'total_rows': 2,
            'valid_rows': 1
        }
    }

    results = get_submission_metadata(sub)
    assert results == expected_results


def test_get_revalidation_threshold(database):
    """ Tests the get_revalidation_threshold function to make sure it returns the correct, properly formatted date """
    sess = database.session

    # Revalidation date
    reval = RevalidationThresholdFactory(revalidation_date=datetime.date(2018, 1, 15))

    sess.add(reval)
    sess.commit()

    results = get_revalidation_threshold()
    assert results['revalidation_threshold'] == '01/15/2018'


def test_get_revalidation_threshold_no_threshold():
    """ Tests the get_revalidation_threshold function to make sure it returns an empty string if there's no date """
    results = get_revalidation_threshold()
    assert results['revalidation_threshold'] == ''


@pytest.mark.usefixtures("job_constants")
def test_get_submission_data_dabs(database):
    """ Tests the get_submission_data function for dabs records """
    sess = database.session

    cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')

    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    sub_2 = SubmissionFactory(submission_id=2, d2_submission=False)

    # Job for submission
    job = JobFactory(job_id=1, submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                     job_status_id=JOB_STATUS_DICT['finished'], file_type_id=FILE_TYPE_DICT['appropriations'],
                     number_of_rows=3, file_size=7655, original_filename='file_1')
    job_2 = JobFactory(job_id=2, submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['file_upload'],
                       job_status_id=JOB_STATUS_DICT['finished'], file_type_id=FILE_TYPE_DICT['program_activity'],
                       number_of_rows=None, file_size=None, original_filename='file_2')
    job_3 = JobFactory(job_id=3, submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                       job_status_id=JOB_STATUS_DICT['running'], file_type_id=FILE_TYPE_DICT['program_activity'],
                       number_of_rows=7, file_size=12345, original_filename='file_2')
    job_4 = JobFactory(job_id=4, submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['validation'],
                       job_status_id=JOB_STATUS_DICT['waiting'], file_type_id=None, number_of_rows=None,
                       file_size=None, original_filename=None)
    job_5 = JobFactory(job_id=5, submission_id=sub_2.submission_id, job_type_id=JOB_TYPE_DICT['validation'],
                       job_status_id=JOB_STATUS_DICT['waiting'], file_type_id=None, number_of_rows=None,
                       file_size=None, original_filename=None)

    sess.add_all([cgac, sub, sub_2, job, job_2, job_3, job_4, job_5])
    sess.commit()

    # a basic csv_validation job, should be in results
    correct_job = {
        'job_id': job.job_id,
        'job_status': job.job_status_name,
        'job_type': job.job_type_name,
        'filename': job.original_filename,
        'file_size': job.file_size,
        'number_of_rows': job.number_of_rows,
        'file_type': job.file_type_name,
        'file_status': "",
        'error_type': "",
        'error_data': [],
        'warning_data': [],
        'missing_headers': [],
        'duplicated_headers': []
    }

    # cross-file job, should be in results
    correct_cross_job = {
        'job_id': job_4.job_id,
        'job_status': job_4.job_status_name,
        'job_type': job_4.job_type_name,
        'filename': None,
        'file_size': None,
        'number_of_rows': None,
        'file_type': '',
        'file_status': "",
        'error_type': "",
        'error_data': [],
        'warning_data': [],
        'missing_headers': [],
        'duplicated_headers': []
    }

    # upload job, shouldn't be in the results
    upload_job = {
        'job_id': job_2.job_id,
        'job_status': job_2.job_status_name,
        'job_type': job_2.job_type_name,
        'filename': job_2.original_filename,
        'file_size': job_2.file_size,
        'number_of_rows': job_2.number_of_rows,
        'file_type': job_2.file_type_name,
        'file_status': "",
        'error_type': "",
        'error_data': [],
        'warning_data': [],
        'missing_headers': [],
        'duplicated_headers': []
    }

    # cross-file job but from another submission, shouldn't be in the results
    different_sub_job = {
        'job_id': job_5.job_id,
        'job_status': job_5.job_status_name,
        'job_type': job_5.job_type_name,
        'filename': job_5.original_filename,
        'file_size': job_5.file_size,
        'number_of_rows': job_5.number_of_rows,
        'file_type': job_5.file_type_name,
        'file_status': "",
        'error_type': "",
        'error_data': [],
        'warning_data': [],
        'missing_headers': [],
        'duplicated_headers': []
    }

    response = get_submission_data(sub)
    response = json.loads(response.data.decode('UTF-8'))
    results = response['jobs']
    assert len(results) == 3
    assert correct_job in results
    assert correct_cross_job in results
    assert upload_job not in results
    assert different_sub_job not in results

    response = get_submission_data(sub, 'appropriations')
    response = json.loads(response.data.decode('UTF-8'))
    results = response['jobs']
    assert len(results) == 1
    assert results[0] == correct_job


@pytest.mark.usefixtures("job_constants")
def test_certify_dabs_submission(database, monkeypatch):
    """ Tests the certify_dabs_submission function """
    with Flask('test-app').app_context():
        now = datetime.datetime.utcnow()
        sess = database.session

        user = UserFactory()
        cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')
        submission = SubmissionFactory(created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                                       reporting_fiscal_period=3, reporting_fiscal_year=2017, is_quarter_format=True,
                                       publishable=True, publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                       d2_submission=False, number_of_errors=0, number_of_warnings=200,
                                       certifying_user_id=None)
        sess.add_all([user, cgac, submission])
        sess.commit()

        g.user = user
        file_handler = fileHandler.FileHandler({}, is_local=True)
        monkeypatch.setattr(file_handler, 'move_certified_files', Mock(return_value=True))
        monkeypatch.setattr(fileHandler.GlobalDB, 'db', Mock(return_value=database))

        certify_dabs_submission(submission, file_handler)

        sess.refresh(submission)
        certify_history = sess.query(CertifyHistory).filter_by(submission_id=submission.submission_id).one_or_none()
        assert certify_history is not None
        assert submission.certifying_user_id == user.user_id
        assert submission.publish_status_id == PUBLISH_STATUS_DICT['published']
