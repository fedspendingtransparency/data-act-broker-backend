from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs30_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'business_funds_indicator', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ BusinessFundsIndicator must contain one of the following values: REC or NON. Case doesn't matter """

    det_award = DetachedAwardFinancialAssistanceFactory(business_funds_indicator='REC', correction_delete_indicatr='c')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(business_funds_indicator='non', correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    det_award_3 = DetachedAwardFinancialAssistanceFactory(business_funds_indicator='red',
                                                          correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3])
    assert errors == 0


def test_failure(database):
    """ BusinessFundsIndicator must contain one of the following values: REC or NON. """

    det_award = DetachedAwardFinancialAssistanceFactory(business_funds_indicator='RECs', correction_delete_indicatr='C')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(business_funds_indicator='red', correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2
