from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import SubTierAgency, CGAC
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd21_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'funding_sub_tier_agency_co'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ FundingSubTierAgencyCode is an optional field, but when provided
    must be a valid 4-digit sub-tier agency code.  """

    subcode = SubTierAgency(sub_tier_agency_code='0000', cgac_id='1')
    cgac = CGAC(cgac_id='1', cgac_code='001', agency_name='test')
    det_award = DetachedAwardFinancialAssistanceFactory(funding_sub_tier_agency_co='0000')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(funding_sub_tier_agency_co=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(funding_sub_tier_agency_co='')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, subcode, cgac])
    assert errors == 0


def test_failure(database):
    """ FundingSubTierAgencyCode is an optional field, but when provided
    must be a valid 4-digit sub-tier agency code.  """

    det_award = DetachedAwardFinancialAssistanceFactory(funding_sub_tier_agency_co='bad')

    errors = number_of_errors(_FILE, database, models=[det_award])
    assert errors == 1
