from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import SubTierAgency, CGAC
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd23_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'awarding_sub_tier_agency_c'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ AwardingSubTierAgencyCode must be a valid 4-digit sub-tier agency code.  """

    agency = SubTierAgency(sub_tier_agency_code='0000', cgac_id='1')
    cgac = CGAC(cgac_id='1', cgac_code='001', agency_name='test')
    det_award = DetachedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c=agency.sub_tier_agency_code)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='0000')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, agency, cgac])
    assert errors == 0


def test_failure(database):
    """ AwardingSubTierAgencyCode must be a valid 4-digit sub-tier agency code.  """

    det_award = DetachedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='bad')

    errors = number_of_errors(_FILE, database, models=[det_award])
    assert errors == 1
