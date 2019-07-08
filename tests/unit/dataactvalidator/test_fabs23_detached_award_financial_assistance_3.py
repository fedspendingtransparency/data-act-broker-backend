from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs23_detached_award_financial_assistance_3'


def test_column_headers(database):
    expected_subset = {'row_number', 'awarding_sub_tier_agency_c', 'awarding_office_code'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ AwardingSubTierAgencyCode must be provided when AwardingOfficeCode is not provided. """

    # Missing office code, has sub tier code
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='000', awarding_office_code='',
                                                          correction_delete_indicatr='')

    # Both codes present
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='000', awarding_office_code='0000',
                                                          correction_delete_indicatr=None)

    # Missing sub tier code, has office code
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c=None, awarding_office_code='0000',
                                                          correction_delete_indicatr='c')

    # Ignore correction delete indicator of D
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='', awarding_office_code='',
                                                          correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_failure(database):
    """ Test failure AwardingSubTierAgencyCode must be provided when AwardingOfficeCode is not provided. """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='', awarding_office_code='',
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c=None, awarding_office_code=None,
                                                          correction_delete_indicatr='c')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 2
