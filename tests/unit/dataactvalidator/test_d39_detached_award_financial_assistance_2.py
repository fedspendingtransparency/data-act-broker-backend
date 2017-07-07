from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd39_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {"row_number", "place_of_performance_code", "place_of_perform_country_c"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ PrimaryPlaceOfPerformanceCode must be 00FORGN when PrimaryPlaceofPerformanceCountryCode is not USA,
        not 00FORGN otherwise. """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="00FORGN",
                                                          place_of_perform_country_c="UKR")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="00FoRGN",
                                                          place_of_perform_country_c="uKr")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="ny**987",
                                                          place_of_perform_country_c="USA")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NY**987",
                                                          place_of_perform_country_c="UsA")
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_failure(database):
    """ Test failure for PrimaryPlaceOfPerformanceCode must be 00FORGN when PrimaryPlaceofPerformanceCountryCode
        is not USA, not 00FORGN otherwise. """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="00FORGN",
                                                          place_of_perform_country_c="USA")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="00FoRGN",
                                                          place_of_perform_country_c="usA")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="ny**987",
                                                          place_of_perform_country_c="UKR")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NY**987",
                                                          place_of_perform_country_c="ukR")
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 4
