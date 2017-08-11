from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs25_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {"row_number", "award_description", "record_type"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ AwardDescription is required for non-aggregate records (i.e., when RecordType = 2). RecordType 1 doesn't
        affect success """
    det_award = DetachedAwardFinancialAssistanceFactory(record_type=2, award_description="Test Description")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=1, award_description="")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(record_type=1, award_description=None)

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3])
    assert errors == 0


def test_failure(database):
    """ AwardDescription is required for non-aggregate records (i.e., when RecordType = 2). """

    det_award = DetachedAwardFinancialAssistanceFactory(record_type=2, award_description="")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=2, award_description=None)

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2
