from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs1_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {"row_number", "record_type", "fain"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test that record_type 1 doesn't affect success (can have no FAIN) and that FAIN works where it's needed"""
    det_award = DetachedAwardFinancialAssistanceFactory(record_type=2, fain="17TCEP0034")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=1, fain="17TCEP0034")
    det_award_null = DetachedAwardFinancialAssistanceFactory(record_type=1, fain=None)

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_null])
    assert errors == 0


def test_failure(database):
    """ Test that a null fain with record type 2 returns an error"""

    det_award = DetachedAwardFinancialAssistanceFactory(record_type=2, fain=None)

    errors = number_of_errors(_FILE, database, models=[det_award])
    assert errors == 1
