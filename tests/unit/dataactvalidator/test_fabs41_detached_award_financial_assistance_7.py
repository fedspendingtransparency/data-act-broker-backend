from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs41_detached_award_financial_assistance_7'


def test_column_headers(database):
    expected_subset = {"row_number", "record_type", "place_of_performance_zip4a"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ PrimaryPlaceOfPerformanceZip+4 must be blank for PII-redacted non-aggregate records (i.e., RecordType = 3)."""

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a="abc", record_type=1)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a="123", record_type=2)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a="", record_type=3)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a=None, record_type=3)
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_failure(database):
    """ Test failure for PrimaryPlaceOfPerformanceZip+4 must be blank for PII-redacted non-aggregate records
        (i.e., RecordType = 3)."""

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a="12345", record_type=3)
    errors = number_of_errors(_FILE, database, models=[det_award_1])
    assert errors == 1
