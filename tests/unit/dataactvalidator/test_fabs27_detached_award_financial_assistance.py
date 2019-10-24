from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs27_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'assistance_type', 'non_federal_funding_amount', 'uniqueid_afa_generated_unique'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ NonFederalFundingAmount must be blank for loans (i.e., when AssistanceType = 07 or 08). """

    det_award = DetachedAwardFinancialAssistanceFactory(assistance_type='07', non_federal_funding_amount=None,
                                                        correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(assistance_type='08', non_federal_funding_amount=None,
                                                          correction_delete_indicatr='c')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(assistance_type='07', non_federal_funding_amount=0,
                                                          correction_delete_indicatr=None)
    # Ignore correction delete indicator of D
    det_award_4 = DetachedAwardFinancialAssistanceFactory(assistance_type='08', non_federal_funding_amount=20,
                                                          correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_failure(database):
    """ NonFederalFundingAmount must be blank for loans (i.e., when AssistanceType = 07 or 08). """

    det_award = DetachedAwardFinancialAssistanceFactory(assistance_type='08', non_federal_funding_amount=20,
                                                        correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[det_award])
    assert errors == 1
