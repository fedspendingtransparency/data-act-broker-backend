from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs41_detached_award_financial_assistance_6'


def test_column_headers(database):
    expected_subset = {"row_number", "place_of_performance_zip4a"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ If PrimaryPlaceOfPerformanceCode is XX00000, PrimaryPlaceOfPerformanceZip4 must not be 'city-wide'"""

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NY00000",
                                                          place_of_performance_zip4a="")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="Ny**123",
                                                          place_of_performance_zip4a="city-wide")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="Ny**123",
                                                          place_of_performance_zip4a='')
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3])
    assert errors == 0


def test_failure(database):
    """ Test failure for if PrimaryPlaceOfPerformanceCode is XX00000, PrimaryPlaceOfPerformanceZip4 must
        not be 'city-wide'"""

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NY00000",
                                                          place_of_performance_zip4a="city-wide")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="VA00000",
                                                          place_of_performance_zip4a="city-wide")
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 2
