from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs36_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'cfda_number', 'uniqueid_afa_generated_unique'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test valid. CFDA_Number must be in XX.XXX format """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number='99.999', correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.345', correction_delete_indicatr='c')
    # Ignore correction delete indicator of D
    det_award_3 = DetachedAwardFinancialAssistanceFactory(cfda_number='1234', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3])
    assert errors == 0


def test_failure(database):
    """ Test invalid. CFDA_Number must be in XX.XXX format """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number='1234', correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.34567', correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.3', correction_delete_indicatr='c')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(cfda_number='123.456', correction_delete_indicatr='C')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(cfda_number='ab.cdf', correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 5
