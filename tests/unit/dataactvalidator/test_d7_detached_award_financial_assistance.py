from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd7_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {"row_number", "record_type", "uri"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests URI is a required field for aggregate records (i.e., when RecordType = 1) """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(record_type=1)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=1, uri="something")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(record_type=2, uri=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(record_type=2, uri="")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_failure(database):
    """ Tests URI is not required field for non-aggregate records (i.e., when RecordType != 1) """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(record_type=1, uri=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=1, uri="")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 2
