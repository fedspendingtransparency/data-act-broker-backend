from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs39_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {"row_number", "record_type", "place_of_performance_code", "place_of_perform_country_c"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ PrimaryPlaceOfPerformanceCode must be 00FORGN when PrimaryPlaceofPerformanceCountryCode is not USA,
        not 00FORGN otherwise for record type 1 and 2. """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="00FORGN",
                                                          place_of_perform_country_c="UKR", record_type=1)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="00FoRGN",
                                                          place_of_perform_country_c="uKr", record_type=1)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="ny**987",
                                                          place_of_perform_country_c="USA", record_type=2)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NY**987",
                                                          place_of_perform_country_c="UsA", record_type=2)
    det_award_5 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NY**987",
                                                          place_of_perform_country_c="UKR", record_type=3)
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ Test failure for PrimaryPlaceOfPerformanceCode must be 00FORGN when PrimaryPlaceofPerformanceCountryCode
        is not USA, not 00FORGN otherwise for record type 1 and 2. """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="00FORGN",
                                                          place_of_perform_country_c="USA", record_type=1)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="00FoRGN",
                                                          place_of_perform_country_c="usA", record_type=1)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="ny**987",
                                                          place_of_perform_country_c="UKR", record_type=2)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NY**987",
                                                          place_of_perform_country_c="ukR", record_type=2)
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 4
