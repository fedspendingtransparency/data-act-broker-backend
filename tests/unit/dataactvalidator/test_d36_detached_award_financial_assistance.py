from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd36_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {"row_number", "cfda_number"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test valid. CFDA_Number must be in XX.XXX format """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number='99.999')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.345')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 0


def test_failure(database):
    """ Test invalid. CFDA_Number must be in XX.XXX format """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number='1234')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.34567')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.3')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(cfda_number='123.456')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(cfda_number='ab.cdf')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 5
