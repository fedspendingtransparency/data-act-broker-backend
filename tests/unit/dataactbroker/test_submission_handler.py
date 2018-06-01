import pytest

import datetime

from dataactbroker.handlers.submission_handler import get_submission_metadata

from dataactcore.models.lookups import PUBLISH_STATUS_DICT
from dataactcore.models.jobModels import FileType, JobStatus, JobType

from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory, JobFactory, CertifyHistoryFactory
from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory


@pytest.mark.usefixtures("job_constants")
def test_get_submission_metadata(database):
    """ Tests the get_submission_metadata function """
    sess = database.session

    now = datetime.datetime.utcnow()
    now_plus_10 = now + datetime.timedelta(minutes=10)
    start_date = datetime.date(2000, 1, 1)
    cgac = CGACFactory(cgac_code='001', agency_name='CGAC Agency')
    frec_cgac = CGACFactory(cgac_code='999', agency_name='FREC CGAC')
    frec = FRECFactory(frec_code='0001', agency_name='FREC Agency', cgac=frec_cgac)
    # Quarterly DABS cgac corrected
    sub_1 = SubmissionFactory(submission_id='1', created_at=now, updated_at=now_plus_10, cgac_code=cgac.cgac_code,
                              reporting_fiscal_period=3, reporting_fiscal_year=2017, is_quarter_format=True,
                              publish_status_id=PUBLISH_STATUS_DICT['updated'], d2_submission=False)
    # Quarterly DABS frec published
    sub_2 = SubmissionFactory(submission_id='2', created_at=now, updated_at=now, cgac_code=None,
                              frec_code=frec.frec_code, reporting_fiscal_period=6, reporting_fiscal_year=2010,
                              is_quarter_format=True, publish_status_id=PUBLISH_STATUS_DICT['published'],
                              d2_submission=False)
    # Monthly DABS cgac unpublished
    sub_3 = SubmissionFactory(submission_id='3', created_at=now, updated_at=now_plus_10, cgac_code=cgac.cgac_code,
                              reporting_fiscal_period=4, reporting_fiscal_year=2016, is_quarter_format=False,
                              publish_status_id=PUBLISH_STATUS_DICT['unpublished'], d2_submission=False,
                              reporting_start_date=start_date)
    # FABS unpublished
    sub_4 = SubmissionFactory(submission_id='4', created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                              reporting_fiscal_period=1, reporting_fiscal_year=2015, is_quarter_format=False,
                              publish_status_id=PUBLISH_STATUS_DICT['unpublished'], d2_submission=True,
                              reporting_start_date=start_date)
    # FABS published
    sub_5 = SubmissionFactory(submission_id='5', created_at=now, updated_at=now, cgac_code=cgac.cgac_code,
                              reporting_fiscal_period=5, reporting_fiscal_year=2010, is_quarter_format=False,
                              publish_status_id=PUBLISH_STATUS_DICT['published'], d2_submission=True,
                              reporting_start_date=start_date)
    # Job for submission 1
    job_1 = JobFactory(submission_id=sub_1.submission_id, last_validated=now_plus_10,
                       job_type=sess.query(JobType).filter_by(name='validation').one(),
                       job_status=sess.query(JobStatus).filter_by(name='finished').one(),
                       file_type=sess.query(FileType).filter_by(name='appropriations').one())
    # Data for FABS
    dafa_1 = DetachedAwardFinancialAssistanceFactory(submission_id=sub_5.submission_id, is_valid=True)
    dafa_2 = DetachedAwardFinancialAssistanceFactory(submission_id=sub_5.submission_id, is_valid=False)
    cert_hist = CertifyHistoryFactory(submission=sub_5, created_at=now_plus_10)

    sess.add_all([cgac, frec_cgac, frec, sub_1, sub_2, sub_3, sub_4, sub_5, job_1, dafa_1, dafa_2, cert_hist])
    sess.commit()

    # Test for Quarterly, updated DABS cgac submission
    expected_results = {
        'cgac_code': cgac.cgac_code,
        'frec_code': None,
        'agency_name': cgac.agency_name,
        'created_on': now.strftime('%m/%d/%Y'),
        'last_updated': now_plus_10.strftime("%Y-%m-%dT%H:%M:%S"),
        'last_validated': now_plus_10.strftime('%m/%d/%Y'),
        'reporting_period': 'Q1/2017',
        'publish_status': 'updated',
        'quarterly_submission': True,
        'fabs_meta': None
    }

    results = get_submission_metadata(sub_1)
    assert results == expected_results

    # Test for Quarterly, published DABS frec submission
    expected_results.update({
        'cgac_code': None,
        'frec_code': frec.frec_code,
        'agency_name': frec.agency_name,
        'last_updated': now.strftime("%Y-%m-%dT%H:%M:%S"),
        'last_validated': '',
        'reporting_period': 'Q2/2010',
        'publish_status': 'published'
    })

    results = get_submission_metadata(sub_2)
    assert results == expected_results

    # Test for monthly, unpublished DABS cgac submission
    expected_results.update({
        'cgac_code': cgac.cgac_code,
        'frec_code': None,
        'agency_name': cgac.agency_name,
        'last_updated': now_plus_10.strftime("%Y-%m-%dT%H:%M:%S"),
        'reporting_period': start_date.strftime('%m/%Y'),
        'quarterly_submission': False,
        'publish_status': 'unpublished'
    })

    results = get_submission_metadata(sub_3)
    assert results == expected_results

    # Test for unpublished FABS cgac submission
    expected_results.update({
        'last_updated': now.strftime("%Y-%m-%dT%H:%M:%S"),
        'fabs_meta': {'publish_date': None, 'published_file': None, 'total_rows': 0, 'valid_rows': 0}
    })

    results = get_submission_metadata(sub_4)
    assert results == expected_results

    # Test for published FABS cgac submission
    expected_results.update({
        'publish_status': 'published',
        'fabs_meta': {
            'publish_date': now_plus_10.strftime('%-I:%M%p %m/%d/%Y'),
            'published_file': None,
            'total_rows': 2,
            'valid_rows': 1},
    })

    results = get_submission_metadata(sub_5)
    assert results == expected_results
