from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs40_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {"row_number", "place_of_performance_code", "record_type"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ PrimaryPlaceOfPerformanceCode for aggregate records (i.e., when RecordType = 1) must be in countywide
        format (XX**###). """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NY*****", record_type="2")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NY**123", record_type="1")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="ny**987", record_type="1")
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3])
    assert errors == 0


def test_failure(database):
    """ Test failure for PrimaryPlaceOfPerformanceCode for aggregate records (i.e., when RecordType = 1)
        must be in countywide format (XX**###). """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="00**333", record_type="1")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="AB**33", record_type="1")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="00*****", record_type="1")
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3])
    assert errors == 3
