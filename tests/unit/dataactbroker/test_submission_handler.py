import datetime
import pytest
import json

from flask import Flask, g
from unittest.mock import Mock

from dataactbroker.handlers import fileHandler
from dataactbroker.handlers.submission_handler import (
    publish_checks, process_dabs_publish, process_dabs_certify, publish_dabs_submission,
    publish_and_certify_dabs_submission, get_submission_metadata, get_revalidation_threshold, get_submission_data,
    move_published_data, get_latest_publication_period, revert_to_certified)

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.lookups import (PUBLISH_STATUS_DICT, JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT,
                                        FILE_STATUS_DICT)
from dataactcore.models.errorModels import ErrorMetadata, CertifiedErrorMetadata, File
from dataactcore.models.jobModels import (CertifyHistory, PublishHistory, CertifiedComment, Job, Submission,
                                          PublishedFilesHistory)
from dataactcore.models.stagingModels import (Appropriation, ObjectClassProgramActivity, AwardFinancial,
                                              CertifiedAppropriation, CertifiedObjectClassProgramActivity,
                                              CertifiedAwardFinancial, FlexField, CertifiedFlexField)
from dataactcore.utils.responseException import ResponseException

from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory
from tests.unit.dataactcore.factories.job import (SubmissionFactory, JobFactory, CertifyHistoryFactory,
                                                  PublishHistoryFactory, RevalidationThresholdFactory,
                                                  SubmissionWindowScheduleFactory, CommentFactory)
from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactcore.factories.user import UserFactory


@pytest.mark.usefixtures('job_constants')
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
        'number_of_rows': 8,
        'total_size': 20000,
        'created_on': now.strftime('%Y-%m-%dT%H:%M:%S'),
        'last_updated': now_plus_10.strftime('%Y-%m-%dT%H:%M:%S'),
        'last_validated': now_plus_10.strftime('%Y-%m-%dT%H:%M:%S'),
        'reporting_period': 'Q1/2017',
        'reporting_start_date': sub.reporting_start_date.strftime('%m/%d/%Y'),
        'reporting_end_date': sub.reporting_end_date.strftime('%m/%d/%Y'),
        'publish_status': 'updated',
        'quarterly_submission': True,
        'test_submission': False,
        'published_submission_ids': [],
        'certified': False,
        'certification_deadline': '',
        'fabs_submission': False,
        'fabs_meta': None
    }

    results = get_submission_metadata(sub)
    assert results == expected_results


@pytest.mark.usefixtures('job_constants')
def test_get_submission_metadata_quarterly_dabs_frec(database):
    """ Tests the get_submission_metadata function for quarterly dabs submissions frec """
    sess = database.session

    now = datetime.datetime.utcnow()
    frec_cgac = CGACFactory(cgac_code='999', agency_name='FREC CGAC')
    frec = FRECFactory(frec_code='0001', agency_name='FREC Agency', cgac=frec_cgac)

    sub = SubmissionFactory(submission_id=2, created_at=now, updated_at=now, cgac_code=None, frec_code=frec.frec_code,
                            reporting_fiscal_period=6, reporting_fiscal_year=2010, is_quarter_format=True,
                            publish_status_id=PUBLISH_STATUS_DICT['published'], d2_submission=False, number_of_errors=0,
                            number_of_warnings=0, certified=True)

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
        'created_on': now.strftime('%Y-%m-%dT%H:%M:%S'),
        'last_updated': now.strftime('%Y-%m-%dT%H:%M:%S'),
        'last_validated': '',
        'reporting_period': 'Q2/2010',
        'reporting_start_date': sub.reporting_start_date.strftime('%m/%d/%Y'),
        'reporting_end_date': sub.reporting_end_date.strftime('%m/%d/%Y'),
        'publish_status': 'published',
        'quarterly_submission': True,
        'test_submission': False,
        'published_submission_ids': [],
        'certified': True,
        'certification_deadline': '',
        'fabs_submission': False,
        'fabs_meta': None
    }

    results = get_submission_metadata(sub)
    assert results == expected_results


@pytest.mark.usefixtures('job_constants')
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
        'created_on': now.strftime('%Y-%m-%dT%H:%M:%S'),
        'last_updated': now_plus_10.strftime('%Y-%m-%dT%H:%M:%S'),
        'last_validated': '',
        'reporting_period': 'P04/2016',
        'reporting_start_date': sub.reporting_start_date.strftime('%m/%d/%Y'),
        'reporting_end_date': sub.reporting_end_date.strftime('%m/%d/%Y'),
        'publish_status': 'unpublished',
        'quarterly_submission': False,
        'test_submission': False,
        'published_submission_ids': [],
        'certified': False,
        'certification_deadline': '',
        'fabs_submission': False,
        'fabs_meta': None
    }

    results = get_submission_metadata(sub)
    assert results == expected_results


@pytest.mark.usefixtures('job_constants')
def test_get_submission_metadata_unpublished_fabs(database):
    """ Tests the get_submission_metadata function for unpublished fabs submissions """
    sess = database.session

    now = datetime.datetime.utcnow()
    start_date = datetime.date(2000, 1, 1)
    cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')
    frec_cgac = CGACFactory(cgac_code='999', agency_name='FREC CGAC')
    frec = FRECFactory(frec_code='0001', agency_name='FREC Agency', cgac=frec_cgac)

    sub = SubmissionFactory(submission_id=4, created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                            reporting_fiscal_period=2, reporting_fiscal_year=2015, is_quarter_format=False,
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
        'created_on': now.strftime('%Y-%m-%dT%H:%M:%S'),
        'last_updated': now.strftime('%Y-%m-%dT%H:%M:%S'),
        'last_validated': '',
        'reporting_period': 'P01-P02/2015',
        'reporting_start_date': sub.reporting_start_date.strftime('%m/%d/%Y'),
        'reporting_end_date': sub.reporting_end_date.strftime('%m/%d/%Y'),
        'publish_status': 'unpublished',
        'quarterly_submission': False,
        'test_submission': False,
        'published_submission_ids': [],
        'certified': False,
        'certification_deadline': '',
        'fabs_submission': True,
        'fabs_meta': {'publish_date': None, 'published_file': None, 'total_rows': 0, 'valid_rows': 0}
    }

    results = get_submission_metadata(sub)
    assert results == expected_results


@pytest.mark.usefixtures('job_constants')
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
    pub_hist = PublishHistoryFactory(submission=sub, created_at=now_plus_10)

    sess.add_all([cgac, frec_cgac, frec, sub, dafa_1, dafa_2, cert_hist, pub_hist])
    sess.commit()

    expected_results = {
        'cgac_code': cgac.cgac_code,
        'frec_code': None,
        'agency_name': cgac.agency_name,
        'number_of_errors': 0,
        'number_of_warnings': 2,
        'number_of_rows': 0,
        'total_size': 0,
        'created_on': now.strftime('%Y-%m-%dT%H:%M:%S'),
        'last_updated': now.strftime('%Y-%m-%dT%H:%M:%S'),
        'last_validated': '',
        'reporting_period': 'P05/2010',
        'reporting_start_date': sub.reporting_start_date.strftime('%m/%d/%Y'),
        'reporting_end_date': sub.reporting_end_date.strftime('%m/%d/%Y'),
        'publish_status': 'published',
        'quarterly_submission': False,
        'test_submission': False,
        'published_submission_ids': [],
        'certified': False,
        'certification_deadline': '',
        'fabs_submission': True,
        'fabs_meta': {
            'publish_date': now_plus_10.strftime('%Y-%m-%dT%H:%M:%S'),
            'published_file': None,
            'total_rows': 2,
            'valid_rows': 1
        }
    }

    results = get_submission_metadata(sub)
    assert results == expected_results


@pytest.mark.usefixtures('job_constants')
def test_get_submission_metadata_test_submission(database):
    """ Tests the get_submission_metadata function for published fabs submissions """
    sess = database.session

    now = datetime.datetime.utcnow()
    cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')

    sub1 = SubmissionFactory(submission_id=1, created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                             reporting_fiscal_period=3, reporting_fiscal_year=2017, is_quarter_format=True,
                             publish_status_id=PUBLISH_STATUS_DICT['updated'], d2_submission=False, number_of_errors=40,
                             number_of_warnings=200)
    sub2 = SubmissionFactory(submission_id=2, created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                             reporting_fiscal_period=3, reporting_fiscal_year=2017, is_quarter_format=True,
                             publish_status_id=PUBLISH_STATUS_DICT['unpublished'], d2_submission=False,
                             number_of_errors=40, number_of_warnings=200, test_submission=True,
                             published_submission_ids=[sub1.submission_id])

    sess.add_all([cgac, sub1, sub2])
    sess.commit()

    # Test for test submission
    expected_results = {
        'cgac_code': cgac.cgac_code,
        'frec_code': None,
        'agency_name': cgac.agency_name,
        'number_of_errors': 40,
        'number_of_warnings': 200,
        'number_of_rows': 0,
        'total_size': 0,
        'created_on': now.strftime('%Y-%m-%dT%H:%M:%S'),
        'last_updated': now.strftime('%Y-%m-%dT%H:%M:%S'),
        'last_validated': '',
        'reporting_period': 'Q1/2017',
        'reporting_start_date': sub2.reporting_start_date.strftime('%m/%d/%Y'),
        'reporting_end_date': sub2.reporting_end_date.strftime('%m/%d/%Y'),
        'publish_status': 'unpublished',
        'quarterly_submission': True,
        'test_submission': True,
        'published_submission_ids': [1],
        'certified': False,
        'certification_deadline': '',
        'fabs_submission': False,
        'fabs_meta': None
    }

    results = get_submission_metadata(sub2)
    assert results == expected_results


def test_get_revalidation_threshold(database):
    """ Tests the get_revalidation_threshold function to make sure it returns the correct, properly formatted date """
    sess = database.session

    # Revalidation date
    reval = RevalidationThresholdFactory(revalidation_date=datetime.datetime(2018, 1, 15, 0, 0))

    sess.add(reval)
    sess.commit()

    results = get_revalidation_threshold()
    assert results['revalidation_threshold'] == '2018-01-15T00:00:00'


def test_get_revalidation_threshold_no_threshold():
    """ Tests the get_revalidation_threshold function to make sure it returns an empty string if there's no date """
    results = get_revalidation_threshold()
    assert results['revalidation_threshold'] == ''


def test_get_latest_publication_period(database):
    """ Tests the get_latest_publication_period function to make sure it returns the correct period and year """
    sess = database.session

    # Revalidation date
    today = datetime.datetime.today()
    reval1 = SubmissionWindowScheduleFactory(period=3, year=2016, period_start=today - datetime.timedelta(1))
    reval2 = SubmissionWindowScheduleFactory(period=6, year=2016, period_start=today)
    reval3 = SubmissionWindowScheduleFactory(period=9, year=2017, period_start=today + datetime.timedelta(1))
    sess.add_all([reval1, reval2, reval3])
    sess.commit()

    results = get_latest_publication_period()
    assert results['period'] == 6
    assert results['year'] == 2016


def test_get_latest_publication_period_no_threshold():
    """ Tests the get_latest_publication_period function to make sure it returns Nones if there's no prior period """
    results = get_latest_publication_period()
    assert results['period'] is None
    assert results['year'] is None


@pytest.mark.usefixtures('job_constants')
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
        'number_of_rows': job.number_of_rows - 1,
        'file_type': job.file_type_name,
        'file_status': '',
        'error_type': '',
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
        'number_of_rows': 0,
        'file_type': '',
        'file_status': '',
        'error_type': '',
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
        'file_status': '',
        'error_type': '',
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
        'file_status': '',
        'error_type': '',
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


@pytest.mark.usefixtures('job_constants')
def test_publish_and_certify_dabs_submission(database, monkeypatch):
    """ Tests the publish_and_certify_dabs_submission function """
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
        sub_window = SubmissionWindowScheduleFactory(year=2017, period=3, period_start=now - datetime.timedelta(days=1))
        sess.add_all([user, cgac, submission, sub_window])
        sess.commit()

        comment = CommentFactory(file_type_id=FILE_TYPE_DICT['appropriations'], comment='Test',
                                 submission_id=submission.submission_id)
        job_1 = JobFactory(submission_id=submission.submission_id, last_validated=now,
                           job_type_id=JOB_TYPE_DICT['csv_record_validation'])
        job_2 = JobFactory(submission_id=submission.submission_id, last_validated=now + datetime.timedelta(days=1),
                           job_type_id=JOB_TYPE_DICT['csv_record_validation'])
        sess.add_all([job_1, job_2, comment])
        sess.commit()

        flex_field = FlexField(file_type_id=FILE_TYPE_DICT['appropriations'], header='flex_test', job_id=job_1.job_id,
                               submission_id=submission.submission_id, row_number=2, cell=None)
        sess.add(flex_field)
        sess.commit()

        g.user = user
        file_handler = fileHandler.FileHandler({}, is_local=True)
        monkeypatch.setattr(file_handler, 'move_published_files', Mock(return_value=True))
        monkeypatch.setattr(fileHandler.GlobalDB, 'db', Mock(return_value=database))

        publish_and_certify_dabs_submission(submission, file_handler)

        sess.refresh(submission)
        certify_history = sess.query(CertifyHistory).filter_by(submission_id=submission.submission_id).one_or_none()
        publish_history = sess.query(PublishHistory).filter_by(submission_id=submission.submission_id).one_or_none()
        assert certify_history is not None
        assert publish_history is not None
        assert submission.certifying_user_id == user.user_id
        assert submission.publish_status_id == PUBLISH_STATUS_DICT['published']
        assert submission.certified is True

        # Make sure certified comments are created
        certified_comment = sess.query(CertifiedComment).filter_by(submission_id=submission.submission_id).one_or_none()
        assert certified_comment is not None

        # Make sure certified flex fields are created
        certified_flex = sess.query(CertifiedFlexField).filter_by(submission_id=submission.submission_id).one_or_none()
        assert certified_flex is not None


@pytest.mark.usefixtures('job_constants')
def test_published_submission_ids_month_same_periods(database, monkeypatch):
    """ When publishing a monthly submission, other submissions in the same period will update. We just need to test
        the process_dabs_publish function here since it's shared
    """
    with Flask('test-app').app_context():
        now = datetime.datetime.utcnow()
        sess = database.session

        user = UserFactory()
        cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')
        pub_mon1_submission = SubmissionFactory(created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                                                reporting_fiscal_period=1, reporting_fiscal_year=2017,
                                                is_quarter_format=False, publishable=True,
                                                publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                                d2_submission=False, number_of_errors=0, number_of_warnings=200,
                                                certifying_user_id=None)
        pub_mon2_submission = SubmissionFactory(created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                                                reporting_fiscal_period=2, reporting_fiscal_year=2017,
                                                is_quarter_format=False, publishable=True,
                                                publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                                d2_submission=False, number_of_errors=0, number_of_warnings=200,
                                                certifying_user_id=None)
        non_pub_same_mon_submission = SubmissionFactory(created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                                                        reporting_fiscal_period=1, reporting_fiscal_year=2017,
                                                        is_quarter_format=False, publishable=True,
                                                        publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                                        d2_submission=False, number_of_errors=0, number_of_warnings=200,
                                                        certifying_user_id=None)
        non_pub_diff_mon_submission = SubmissionFactory(created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                                                        reporting_fiscal_period=3, reporting_fiscal_year=2017,
                                                        is_quarter_format=False, publishable=True,
                                                        publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                                        d2_submission=False, number_of_errors=0, number_of_warnings=200,
                                                        certifying_user_id=None)
        non_pub_same_qtr_submission = SubmissionFactory(created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                                                        reporting_fiscal_period=3, reporting_fiscal_year=2017,
                                                        is_quarter_format=True, publishable=True,
                                                        publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                                        d2_submission=False, number_of_errors=0, number_of_warnings=200,
                                                        certifying_user_id=None)
        non_pub_diff_qtr_submission = SubmissionFactory(created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                                                        reporting_fiscal_period=6, reporting_fiscal_year=2017,
                                                        is_quarter_format=True, publishable=True,
                                                        publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                                        d2_submission=False, number_of_errors=0, number_of_warnings=200,
                                                        certifying_user_id=None)
        sub_window1 = SubmissionWindowScheduleFactory(year=2017, period=1,
                                                      period_start=now - datetime.timedelta(days=1))
        sub_window2 = SubmissionWindowScheduleFactory(year=2017, period=2,
                                                      period_start=now - datetime.timedelta(days=1))
        sess.add_all([user, cgac, pub_mon1_submission, pub_mon2_submission, non_pub_same_mon_submission,
                      non_pub_diff_mon_submission, non_pub_same_qtr_submission, non_pub_diff_qtr_submission,
                      sub_window1, sub_window2])
        sess.commit()

        job_1 = JobFactory(submission_id=pub_mon1_submission.submission_id, last_validated=now,
                           job_type_id=JOB_TYPE_DICT['csv_record_validation'])
        job_2 = JobFactory(submission_id=pub_mon1_submission.submission_id,
                           last_validated=now + datetime.timedelta(days=1),
                           job_type_id=JOB_TYPE_DICT['csv_record_validation'])
        job_3 = JobFactory(submission_id=pub_mon2_submission.submission_id, last_validated=now,
                           job_type_id=JOB_TYPE_DICT['csv_record_validation'])
        job_4 = JobFactory(submission_id=pub_mon2_submission.submission_id,
                           last_validated=now + datetime.timedelta(days=1),
                           job_type_id=JOB_TYPE_DICT['csv_record_validation'])
        sess.add_all([job_1, job_2, job_3, job_4])
        sess.commit()

        g.user = user
        file_handler = fileHandler.FileHandler({}, is_local=True)
        monkeypatch.setattr(file_handler, 'move_published_files', Mock(return_value=True))
        monkeypatch.setattr(fileHandler.GlobalDB, 'db', Mock(return_value=database))

        process_dabs_publish(pub_mon1_submission, file_handler)
        process_dabs_publish(pub_mon2_submission, file_handler)
        # repeating one to ensure the values don't duplicate
        pub_mon2 = sess.query(Submission).filter(Submission.submission_id == pub_mon2_submission.submission_id).one()
        pub_mon2.publish_status_id = PUBLISH_STATUS_DICT['updated']
        sess.commit()
        process_dabs_publish(pub_mon2_submission, file_handler)

        # monthly same period -> published monthly sub
        sess.refresh(non_pub_same_mon_submission)
        assert non_pub_same_mon_submission.published_submission_ids == [pub_mon1_submission.submission_id]
        assert non_pub_same_mon_submission.test_submission is True
        # monthly different period unaffected
        sess.refresh(non_pub_diff_mon_submission)
        assert non_pub_diff_mon_submission.published_submission_ids == []
        assert non_pub_diff_mon_submission.test_submission is False
        # quarterly same period -> published monthly subs for said quarter
        sess.refresh(non_pub_same_qtr_submission)
        assert non_pub_same_qtr_submission.published_submission_ids == [pub_mon1_submission.submission_id,
                                                                        pub_mon2_submission.submission_id]
        assert non_pub_same_qtr_submission.test_submission is True
        # quarterly different period unaffected
        sess.refresh(non_pub_diff_qtr_submission)
        assert non_pub_diff_qtr_submission.published_submission_ids == []
        assert non_pub_diff_qtr_submission.test_submission is False


@pytest.mark.usefixtures('job_constants')
def test_published_submission_ids_quarter_same_periods(database, monkeypatch):
    """ When publishing a quarterly submission, other submissions in the same period will update """
    with Flask('test-app').app_context():
        now = datetime.datetime.utcnow()
        sess = database.session

        user = UserFactory()
        cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')
        pub_qtr_submission = SubmissionFactory(created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                                               reporting_fiscal_period=3, reporting_fiscal_year=2017,
                                               is_quarter_format=True, publishable=True,
                                               publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                               d2_submission=False, number_of_errors=0, number_of_warnings=200,
                                               certifying_user_id=None)
        non_pub_same_mon_submission = SubmissionFactory(created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                                                        reporting_fiscal_period=1, reporting_fiscal_year=2017,
                                                        is_quarter_format=False, publishable=True,
                                                        publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                                        d2_submission=False, number_of_errors=0, number_of_warnings=200,
                                                        certifying_user_id=None)
        non_pub_diff_mon_submission = SubmissionFactory(created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                                                        reporting_fiscal_period=4, reporting_fiscal_year=2017,
                                                        is_quarter_format=False, publishable=True,
                                                        publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                                        d2_submission=False, number_of_errors=0, number_of_warnings=200,
                                                        certifying_user_id=None)
        non_pub_same_qtr_submission = SubmissionFactory(created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                                                        reporting_fiscal_period=3, reporting_fiscal_year=2017,
                                                        is_quarter_format=True, publishable=True,
                                                        publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                                        d2_submission=False, number_of_errors=0, number_of_warnings=200,
                                                        certifying_user_id=None)
        non_pub_diff_qtr_submission = SubmissionFactory(created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                                                        reporting_fiscal_period=6, reporting_fiscal_year=2017,
                                                        is_quarter_format=True, publishable=True,
                                                        publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                                        d2_submission=False, number_of_errors=0, number_of_warnings=200,
                                                        certifying_user_id=None)
        sub_window = SubmissionWindowScheduleFactory(year=2017, period=3, period_start=now - datetime.timedelta(days=1))
        sess.add_all([user, cgac, pub_qtr_submission, non_pub_same_mon_submission, non_pub_diff_mon_submission,
                      non_pub_same_qtr_submission, non_pub_diff_qtr_submission, sub_window])
        sess.commit()

        job_1 = JobFactory(submission_id=pub_qtr_submission.submission_id, last_validated=now,
                           job_type_id=JOB_TYPE_DICT['csv_record_validation'])
        job_2 = JobFactory(submission_id=pub_qtr_submission.submission_id,
                           last_validated=now + datetime.timedelta(days=1),
                           job_type_id=JOB_TYPE_DICT['csv_record_validation'])
        sess.add_all([job_1, job_2])
        sess.commit()

        g.user = user
        file_handler = fileHandler.FileHandler({}, is_local=True)
        monkeypatch.setattr(file_handler, 'move_published_files', Mock(return_value=True))
        monkeypatch.setattr(fileHandler.GlobalDB, 'db', Mock(return_value=database))

        process_dabs_publish(pub_qtr_submission, file_handler)

        # monthly same quarter -> published quarter submission
        sess.refresh(non_pub_same_mon_submission)
        assert non_pub_same_mon_submission.published_submission_ids == [pub_qtr_submission.submission_id]
        assert non_pub_same_mon_submission.test_submission is True
        # monthly different quarter unaffected
        sess.refresh(non_pub_diff_mon_submission)
        assert non_pub_diff_mon_submission.published_submission_ids == []
        assert non_pub_diff_mon_submission.test_submission is False
        # quarterly same quarter -> published quarter submission
        sess.refresh(non_pub_same_qtr_submission)
        assert non_pub_same_qtr_submission.published_submission_ids == [pub_qtr_submission.submission_id]
        assert non_pub_same_qtr_submission.test_submission is True
        # quarterly different quarter unaffected
        sess.refresh(non_pub_diff_qtr_submission)
        assert non_pub_diff_qtr_submission.published_submission_ids == []
        assert non_pub_diff_qtr_submission.test_submission is False


@pytest.mark.usefixtures('job_constants')
def test_publish_checks_revalidation_needed(database):
    """ Tests the publish_checks function preventing publication when revalidation threshold isn't met """
    now = datetime.datetime.utcnow()
    earlier = now - datetime.timedelta(days=1)
    sess = database.session

    cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')
    submission = SubmissionFactory(created_at=earlier, updated_at=earlier, cgac_code=cgac.cgac_code,
                                   reporting_fiscal_period=3, reporting_fiscal_year=2017, is_quarter_format=True,
                                   publishable=True, publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                   d2_submission=False, number_of_errors=0, number_of_warnings=200,
                                   certifying_user_id=None)
    reval = RevalidationThresholdFactory(revalidation_date=now)
    sess.add_all([cgac, submission, reval])
    sess.commit()
    job = JobFactory(submission_id=submission.submission_id, last_validated=earlier,
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'])
    sess.add(job)
    sess.commit()

    with pytest.raises(ValueError) as val_error:
        publish_checks(submission)

    assert str(val_error.value) == 'This submission has not been validated since before the revalidation ' \
                                   'threshold ({}), it must be revalidated before publishing.'.\
        format(now.strftime('%Y-%m-%d %H:%M:%S'))


@pytest.mark.usefixtures('job_constants')
def test_publish_checks_test_submission(database):
    """ Tests the publish_checks function preventing publication when revalidation threshold isn't met """
    now = datetime.datetime.utcnow()
    earlier = now - datetime.timedelta(days=1)
    sess = database.session

    cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')
    submission = SubmissionFactory(created_at=earlier, updated_at=earlier, cgac_code=cgac.cgac_code,
                                   reporting_fiscal_period=3, reporting_fiscal_year=2017, is_quarter_format=True,
                                   publishable=True, publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                   d2_submission=False, number_of_errors=0, number_of_warnings=200,
                                   certifying_user_id=None)
    reval = RevalidationThresholdFactory(revalidation_date=now)
    sess.add_all([cgac, submission, reval])
    sess.commit()
    job = JobFactory(submission_id=submission.submission_id, last_validated=earlier,
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'])
    sess.add(job)
    sess.commit()

    with pytest.raises(ValueError) as val_error:
        publish_checks(submission)

    assert str(val_error.value) == 'This submission has not been validated since before the revalidation ' \
                                   'threshold ({}), it must be revalidated before publishing.'.\
        format(now.strftime('%Y-%m-%d %H:%M:%S'))


@pytest.mark.usefixtures('job_constants')
def test_publish_checks_window_not_in_db(database):
    """ Tests that a DABS submission that doesnt have its year/period in the system won't be able to certify. """
    now = datetime.datetime.utcnow()
    sess = database.session

    cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')
    submission = SubmissionFactory(created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                                   reporting_fiscal_period=3, reporting_fiscal_year=2017, is_quarter_format=True,
                                   publishable=True, publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                   d2_submission=False, number_of_errors=0, number_of_warnings=200,
                                   certifying_user_id=None)
    sess.add_all([cgac, submission])
    sess.commit()

    job = JobFactory(submission_id=submission.submission_id, last_validated=now,
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'])
    sess.add(job)
    sess.commit()

    with pytest.raises(ValueError) as val_error:
        publish_checks(submission)

    assert str(val_error.value) == 'No submission window for this year and period was found. If this is an error, ' \
                                   'please contact the Service Desk.'


@pytest.mark.usefixtures('job_constants')
def test_publish_checks_window_too_early(database):
    """ Tests that a DABS submission that was last validated before the window start cannot be certified. """
    now = datetime.datetime.utcnow()
    earlier = now - datetime.timedelta(days=1)
    sess = database.session

    cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')
    submission = SubmissionFactory(created_at=earlier, updated_at=earlier, cgac_code=cgac.cgac_code,
                                   reporting_fiscal_period=3, reporting_fiscal_year=2017, is_quarter_format=True,
                                   publishable=True, publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                   d2_submission=False, number_of_errors=0, number_of_warnings=200,
                                   certifying_user_id=None)
    sub_window = SubmissionWindowScheduleFactory(year=2017, period=3, period_start=now)
    sess.add_all([cgac, submission, sub_window])
    sess.commit()

    job = JobFactory(submission_id=submission.submission_id, last_validated=earlier,
                     job_type_id=JOB_TYPE_DICT['csv_record_validation'])
    sess.add(job)
    sess.commit()

    with pytest.raises(ValueError) as val_error:
        publish_checks(submission)

    assert str(val_error.value) == 'This submission was last validated or its D files generated before the ' \
                                   'start of the submission window ({}). Please revalidate before publishing.'.\
        format(sub_window.period_start.strftime('%m/%d/%Y'))


@pytest.mark.usefixtures('job_constants')
def test_publish_and_certify_dabs_submission_window_multiple_thresholds(database):
    """ Tests that a DABS submission is not affected by a different submission window than the one that matches its
        reporting_start_date.
    """
    with Flask('test-app').app_context():
        now = datetime.datetime.utcnow()
        earlier = now - datetime.timedelta(days=1)
        sess = database.session

        user = UserFactory()
        cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')
        submission = SubmissionFactory(created_at=earlier, updated_at=earlier, cgac_code=cgac.cgac_code,
                                       reporting_fiscal_period=3, reporting_fiscal_year=2017,
                                       reporting_start_date='2016-10-01', is_quarter_format=True, publishable=True,
                                       publish_status_id=PUBLISH_STATUS_DICT['unpublished'], d2_submission=False,
                                       number_of_errors=0, number_of_warnings=200, certifying_user_id=None)
        sub_window = SubmissionWindowScheduleFactory(year=2017, period=3, period_start=earlier)
        sub_window_2 = SubmissionWindowScheduleFactory(year=2017, period=6,
                                                       period_start=now + datetime.timedelta(days=10))
        sess.add_all([user, cgac, submission, sub_window, sub_window_2])
        sess.commit()

        job = JobFactory(submission_id=submission.submission_id, last_validated=now,
                         job_type_id=JOB_TYPE_DICT['csv_record_validation'])
        c_job = JobFactory(submission_id=submission.submission_id, last_validated=now,
                           job_type_id=JOB_TYPE_DICT['validation'])
        sess.add_all([job, c_job])
        sess.commit()

        g.user = user
        file_handler = fileHandler.FileHandler({}, is_local=True)
        response = publish_and_certify_dabs_submission(submission, file_handler)
        assert response.status_code == 200


@pytest.mark.usefixtures('job_constants')
def test_publish_checks_reverting(database):
    """ Tests that a DABS submission cannot be certified while reverting. """
    now = datetime.datetime.utcnow()
    earlier = now - datetime.timedelta(days=1)
    sess = database.session

    user = UserFactory()
    cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')
    submission = SubmissionFactory(created_at=earlier, updated_at=earlier, cgac_code=cgac.cgac_code,
                                   reporting_fiscal_period=3, reporting_fiscal_year=2017,
                                   reporting_start_date='2016-10-01', is_quarter_format=True, publishable=True,
                                   publish_status_id=PUBLISH_STATUS_DICT['reverting'], d2_submission=False,
                                   number_of_errors=0, number_of_warnings=200, certifying_user_id=None)
    sess.add_all([user, cgac, submission])
    sess.commit()

    with pytest.raises(ValueError) as val_error:
        publish_checks(submission)

    assert str(val_error.value) == 'Submission is publishing or reverting'


@pytest.mark.usefixtures('job_constants')
def test_publish_dabs_submission_past_due(database):
    """ Tests that a DABS submission cannot be published without recertifying if it is past its certification date """
    now = datetime.datetime.utcnow()
    sess = database.session

    user = UserFactory()
    cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')
    submission = SubmissionFactory(cgac_code=cgac.cgac_code, reporting_fiscal_period=3, reporting_fiscal_year=2017,
                                   reporting_start_date='2016-10-01', is_quarter_format=False, publishable=True,
                                   publish_status_id=PUBLISH_STATUS_DICT['unpublished'], d2_submission=False,
                                   number_of_errors=0, number_of_warnings=200, certifying_user_id=None)
    sched = SubmissionWindowScheduleFactory(period=3, year=2017, period_start=now - datetime.timedelta(5),
                                            certification_deadline=now - datetime.timedelta(1))
    sess.add_all([user, cgac, submission, sched])
    sess.commit()

    file_handler = fileHandler.FileHandler({}, is_local=True)
    results = publish_dabs_submission(submission, file_handler)

    assert results.status_code == 400
    assert results.json['message'] == 'Monthly submissions past their certification deadline must be published and' \
                                      ' certified at the same time. Use the publish_and_certify_dabs_submission' \
                                      ' endpoint.'


@pytest.mark.usefixtures('job_constants')
def test_process_dabs_certify_success(database):
    """ Tests that a DABS submission can be successfully certified and the certify info added to the published files
        history.
    """
    with Flask('test-app').app_context():
        now = datetime.datetime.utcnow()
        earlier = now - datetime.timedelta(days=1)
        sess = database.session

        user = UserFactory()
        cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')
        submission = SubmissionFactory(created_at=earlier, updated_at=earlier, cgac_code=cgac.cgac_code,
                                       reporting_fiscal_period=3, reporting_fiscal_year=2017,
                                       reporting_start_date='2016-10-01', is_quarter_format=False, publishable=True,
                                       publish_status_id=PUBLISH_STATUS_DICT['published'], d2_submission=False,
                                       number_of_errors=0, number_of_warnings=200, certifying_user_id=None)
        sess.add_all([user, cgac, submission])
        sess.commit()

        pub_history = PublishHistory(submission_id=submission.submission_id)
        sess.add(pub_history)
        sess.commit()

        pub_files = PublishedFilesHistory(publish_history_id=pub_history.publish_history_id,
                                          certify_history_id=None,
                                          submission_id=submission.submission_id, filename='old/test/file2.csv',
                                          file_type_id=FILE_TYPE_DICT['appropriations'],
                                          warning_filename='a/warning.csv')
        sess.add(pub_files)
        sess.commit()

        g.user = user
        process_dabs_certify(submission)
        cert_hist = sess.query(CertifyHistory).filter_by(submission_id=submission.submission_id).one_or_none()
        assert cert_hist is not None
        assert pub_files.certify_history_id == cert_hist.certify_history_id


@pytest.mark.usefixtures('job_constants')
def test_process_dabs_certify_no_publish_data(database):
    """ Tests that trying to certify only when there is no published files history data throws an error """
    with Flask('test-app').app_context():
        now = datetime.datetime.utcnow()
        earlier = now - datetime.timedelta(days=1)
        sess = database.session

        user = UserFactory()
        cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')
        submission = SubmissionFactory(created_at=earlier, updated_at=earlier, cgac_code=cgac.cgac_code,
                                       reporting_fiscal_period=3, reporting_fiscal_year=2017,
                                       reporting_start_date='2016-10-01', is_quarter_format=False, publishable=True,
                                       publish_status_id=PUBLISH_STATUS_DICT['published'], d2_submission=False,
                                       number_of_errors=0, number_of_warnings=200, certifying_user_id=None)
        sess.add_all([user, cgac, submission])
        sess.commit()

        g.user = user
        with pytest.raises(ValueError) as val_error:
            process_dabs_certify(submission)

        assert str(val_error.value) == 'There is no publish history associated with this submission. Submission must' \
                                       ' be published before certification.'


@pytest.mark.usefixtures('job_constants')
def test_process_dabs_certify_already_certified(database):
    """ Tests that if this function is somehow reached without a new publication, it throws an error """
    with Flask('test-app').app_context():
        now = datetime.datetime.utcnow()
        earlier = now - datetime.timedelta(days=1)
        sess = database.session

        user = UserFactory()
        cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')
        submission = SubmissionFactory(created_at=earlier, updated_at=earlier, cgac_code=cgac.cgac_code,
                                       reporting_fiscal_period=3, reporting_fiscal_year=2017,
                                       reporting_start_date='2016-10-01', is_quarter_format=False, publishable=True,
                                       publish_status_id=PUBLISH_STATUS_DICT['published'], d2_submission=False,
                                       number_of_errors=0, number_of_warnings=200, certifying_user_id=None)
        sess.add_all([user, cgac, submission])
        sess.commit()

        pub_history = PublishHistory(submission_id=submission.submission_id)
        cert_history = CertifyHistory(submission_id=submission.submission_id)
        sess.add_all([pub_history, cert_history])
        sess.commit()

        pub_files = PublishedFilesHistory(publish_history_id=pub_history.publish_history_id,
                                          certify_history_id=cert_history.certify_history_id,
                                          submission_id=submission.submission_id, filename='old/test/file2.csv',
                                          file_type_id=FILE_TYPE_DICT['appropriations'],
                                          warning_filename='a/warning.csv')
        sess.add(pub_files)
        sess.commit()

        g.user = user
        with pytest.raises(ValueError) as val_error:
            process_dabs_certify(submission)

        assert str(val_error.value) == 'This submission already has a certification associated with the most recent' \
                                       ' publication.'


@pytest.mark.usefixtures('error_constants')
@pytest.mark.usefixtures('job_constants')
def test_revert_submission(database, monkeypatch):
    """ Tests reverting an updated DABS certification """
    with Flask('test-app').app_context():
        sess = database.session

        sub = Submission(publish_status_id=PUBLISH_STATUS_DICT['updated'], is_quarter_format=True, d2_submission=False,
                         publishable=False, number_of_errors=20, number_of_warnings=15)
        sess.add(sub)
        sess.commit()

        job = Job(submission_id=sub.submission_id, job_status_id=JOB_STATUS_DICT['finished'],
                  job_type_id=JOB_TYPE_DICT['csv_record_validation'], file_type_id=FILE_TYPE_DICT['appropriations'],
                  number_of_warnings=0, number_of_errors=10, filename='new/test/file.csv', number_of_rows=5,
                  number_of_rows_valid=0)
        pub_history = PublishHistory(submission_id=sub.submission_id)
        sess.add_all([job, pub_history])
        sess.commit()

        cert_approp = CertifiedAppropriation(submission_id=sub.submission_id, job_id=job.job_id, row_number=1,
                                             spending_authority_from_of_cpe=2, tas='test')
        approp = Appropriation(submission_id=sub.submission_id, job_id=job.job_id, row_number=1,
                               spending_authority_from_of_cpe=15, tas='test')
        pub_files = PublishedFilesHistory(publish_history_id=pub_history.publish_history_id,
                                          certify_history_id=None,
                                          submission_id=sub.submission_id, filename='old/test/file2.csv',
                                          file_type_id=FILE_TYPE_DICT['appropriations'],
                                          warning_filename='a/warning.csv')
        cert_meta1 = CertifiedErrorMetadata(job_id=job.job_id, file_type_id=FILE_TYPE_DICT['appropriations'],
                                            target_file_type_id=None, occurrences=15)
        cert_meta2 = CertifiedErrorMetadata(job_id=job.job_id, file_type_id=FILE_TYPE_DICT['appropriations'],
                                            target_file_type_id=None, occurrences=10)
        file_entry = File(file_id=FILE_TYPE_DICT['appropriations'], job_id=job.job_id,
                          file_status_id=FILE_STATUS_DICT['incomplete'], headers_missing='something')
        sess.add_all([cert_approp, approp, pub_files, cert_meta1, cert_meta2, file_entry])
        sess.commit()

        file_handler = fileHandler.FileHandler({}, is_local=True)
        monkeypatch.setattr(file_handler, 'revert_published_error_files', Mock())
        revert_to_certified(sub, file_handler)

        # Test that published data is moved back
        approp_query = sess.query(Appropriation).filter_by(submission_id=sub.submission_id).all()
        assert len(approp_query) == 1
        assert approp_query[0].spending_authority_from_of_cpe == 2

        # Test that the job got updated
        job_query = sess.query(Job).filter_by(submission_id=sub.submission_id).all()
        assert len(job_query) == 1
        assert job_query[0].filename == CONFIG_BROKER['broker_files'] + 'file2.csv'
        assert job_query[0].number_of_warnings == 25
        assert job_query[0].number_of_errors == 0
        assert job_query[0].number_of_rows == 2
        assert job_query[0].number_of_rows_valid == 1

        # Test that File got updated
        file_query = sess.query(File).filter_by(job_id=job.job_id).all()
        assert len(file_query) == 1
        assert file_query[0].headers_missing is None
        assert file_query[0].file_status_id == FILE_STATUS_DICT['complete']

        # Make sure submission got updated
        sub_query = sess.query(Submission).filter_by(submission_id=sub.submission_id).all()
        assert len(sub_query) == 1
        assert sub_query[0].publishable is True
        assert sub_query[0].number_of_errors == 0
        assert sub_query[0].number_of_warnings == 25


@pytest.mark.usefixtures('job_constants')
def test_revert_submission_fabs_submission(database):
    """ Tests reverting an updated DABS certification failure for FABS submission """
    sess = database.session

    sub = Submission(d2_submission=True)
    sess.add(sub)
    sess.commit()

    file_handler = fileHandler.FileHandler({}, is_local=True)
    with pytest.raises(ResponseException) as resp_except:
        revert_to_certified(sub, file_handler)

    assert resp_except.value.status == 400
    assert str(resp_except.value) == 'Submission must be a DABS submission.'


@pytest.mark.usefixtures('job_constants')
def test_revert_submission_not_updated_submission(database):
    """ Tests reverting an updated DABS certification failure for non-updated submission """
    sess = database.session

    sub1 = Submission(publish_status_id=PUBLISH_STATUS_DICT['published'], d2_submission=False)
    sub2 = Submission(publish_status_id=PUBLISH_STATUS_DICT['unpublished'], d2_submission=False)
    sess.add_all([sub1, sub2])
    sess.commit()

    file_handler = fileHandler.FileHandler({}, is_local=True)
    # Published submission
    with pytest.raises(ResponseException) as resp_except:
        revert_to_certified(sub1, file_handler)

    assert resp_except.value.status == 400
    assert str(resp_except.value) == 'Submission has not been published or has not been updated since publication.'

    # Unpublished submission
    with pytest.raises(ResponseException) as resp_except:
        revert_to_certified(sub2, file_handler)

    assert resp_except.value.status == 400
    assert str(resp_except.value) == 'Submission has not been published or has not been updated since publication.'


@pytest.mark.usefixtures('job_constants')
def test_move_published_data(database):
    """ Tests the move_published_data function """
    with Flask('test-app').app_context():
        sess = database.session

        # Create 2 submissions
        sub_1 = SubmissionFactory()
        sub_2 = SubmissionFactory()
        sess.add_all([sub_1, sub_2])
        sess.commit()

        # Create jobs so we can put a job ID into the tables
        job_1 = JobFactory(submission_id=sub_1.submission_id)
        job_2 = JobFactory(submission_id=sub_2.submission_id)
        sess.add_all([job_1, job_2])
        sess.commit()

        # Create Appropriation entries, 1 per submission, and one of each other kind
        approp_1 = Appropriation(submission_id=sub_1.submission_id, job_id=job_1.job_id, row_number=1,
                                 spending_authority_from_of_cpe=2)
        approp_2 = Appropriation(submission_id=sub_2.submission_id, job_id=job_2.job_id, row_number=1,
                                 spending_authority_from_of_cpe=2)
        ocpa = ObjectClassProgramActivity(submission_id=sub_1.submission_id, job_id=job_1.job_id, row_number=1)
        award_fin = AwardFinancial(submission_id=sub_1.submission_id, job_id=job_1.job_id, row_number=1)
        error_1 = ErrorMetadata(job_id=job_1.job_id)
        error_2 = ErrorMetadata(job_id=job_2.job_id)
        sess.add_all([approp_1, approp_2, ocpa, award_fin, error_1, error_2])
        sess.commit()

        move_published_data(sess, sub_1.submission_id)

        # There are 2 entries, we only want to move the 1 with the submission ID that matches
        approp_query = sess.query(CertifiedAppropriation).filter_by(submission_id=sub_1.submission_id).all()
        assert len(approp_query) == 1
        assert approp_query[0].spending_authority_from_of_cpe == 2

        # Make sure the others got moved as well
        ocpa_query = sess.query(CertifiedObjectClassProgramActivity).filter_by(submission_id=sub_1.submission_id).all()
        award_query = sess.query(CertifiedAwardFinancial).filter_by(submission_id=sub_1.submission_id).all()
        # Query all job IDs but only one result should show up
        error_query = sess.query(CertifiedErrorMetadata).\
            filter(CertifiedErrorMetadata.job_id.in_([job_1.job_id, job_2.job_id])).all()
        assert len(ocpa_query) == 1
        assert len(award_query) == 1
        assert len(error_query) == 1

        # Change the Appropriation data
        approp_1.spending_authority_from_of_cpe = 5
        sess.refresh(approp_1)

        # Move the data again (republish) and make sure we didn't add extras, just adjusted the one we had
        move_published_data(sess, sub_1.submission_id)
        approp_query = sess.query(CertifiedAppropriation).filter_by(submission_id=sub_1.submission_id).all()
        assert len(approp_query) == 1
        assert approp_query[0].spending_authority_from_of_cpe == 2
