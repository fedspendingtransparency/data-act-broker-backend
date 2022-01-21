from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs31_detached_award_financial_assistance_3'


def test_column_headers(database):
    expected_subset = {'row_number', 'uei', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test success for when AwardeeOrRecipientUEI is provided, it must be twelve characters. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(uei='123456789aBc', correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(uei='abc000000000', correction_delete_indicatr='C')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(uei='000000000000', correction_delete_indicatr=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(uei=None, correction_delete_indicatr=None)

    # Ignore correction delete indicator of D
    det_award_5 = DetachedAwardFinancialAssistanceFactory(uei='2', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ Test failure for when AwardeeOrRecipientUEI is provided, it must be twelve characters. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(uei='2', correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(uei='1234567s89aBc', correction_delete_indicatr='c')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 2
