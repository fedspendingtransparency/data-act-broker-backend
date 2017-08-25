from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs6_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {"row_number", "record_type"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests success for when Record type is required and cannot be blank. It must be 1 or 2 """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(record_type=1)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=2)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 0


def test_failure(database):
    """ Tests failure for when Record type is required and cannot be blank. It must be 1 or 2 """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(record_type=0)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=None)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 2
