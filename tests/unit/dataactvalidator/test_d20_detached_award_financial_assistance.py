from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import CGAC
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd20_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'funding_agency_code'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ FundingAgencyCode is an optional field, but when provided
    must be a valid 3-digit CGAC agency code. """

    cgac = CGAC(cgac_code='001')
    det_award = DetachedAwardFinancialAssistanceFactory(funding_agency_code=cgac.cgac_code)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(funding_agency_code=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(funding_agency_code='')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, cgac])
    assert errors == 0


def test_failure(database):
    """ FundingAgencyCode is an optional field, but when provided
    must be a valid 3-digit CGAC agency code. """

    cgac = CGAC(cgac_code='001')
    det_award = DetachedAwardFinancialAssistanceFactory(funding_agency_code='bad')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(funding_agency_code='12345')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, cgac])
    assert errors == 2
