from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import CGAC
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd22_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'awarding_agency_code', 'awarding_sub_tier_agency_c'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ AwardingAgencyCode is optional when provided must be a valid 3-digit CGAC agency code
    If the AwardingAgencyCode is blank, it is auto-populated from the AwardingSubTierAgencyCode """

    cgac = CGAC(cgac_code='001')
    det_award = DetachedAwardFinancialAssistanceFactory(awarding_agency_code=cgac.cgac_code,
                                                        awarding_sub_tier_agency_c='0001')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awarding_agency_code='',
                                                          awarding_sub_tier_agency_c='')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, cgac])
    assert errors == 0


def test_failure(database):
    """ AwardingAgencyCode is optional when provided must be a valid 3-digit CGAC agency code
    If the AwardingAgencyCode is blank, it is auto-populated from the AwardingSubTierAgencyCode """

    cgac = CGAC(cgac_code='001')
    det_award = DetachedAwardFinancialAssistanceFactory(awarding_agency_code='bad',
                                                        awarding_sub_tier_agency_c='1234')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awarding_agency_code='12345',
                                                          awarding_sub_tier_agency_c='1234')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awarding_agency_code='',
                                                          awarding_sub_tier_agency_c='1234')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, cgac])
    assert errors == 3
