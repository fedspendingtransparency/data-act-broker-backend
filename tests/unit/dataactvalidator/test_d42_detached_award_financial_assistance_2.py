from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd42_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {"row_number", "place_of_performance_forei", "place_of_perform_country_c"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test PrimaryPlaceOfPerformanceForeignLocationDescription must be blank for domestic recipients
        (i.e., when PrimaryPlaceOfPerformanceCountryCode = USA). This test shouldn't care about content when
        country_code is not USA (that is for another validation). """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_forei="description",
                                                          place_of_perform_country_c="UK")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_forei=None,
                                                          place_of_perform_country_c="UK")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_forei=None,
                                                          place_of_perform_country_c="USA")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_forei="",
                                                          place_of_perform_country_c="UsA")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_failure(database):
    """ Test failure PrimaryPlaceOfPerformanceForeignLocationDescription must be blank for domestic recipients
        (i.e., when PrimaryPlaceOfPerformanceCountryCode = USA). """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_forei="Test",
                                                          place_of_perform_country_c="USA")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_forei="Content",
                                                          place_of_perform_country_c="usa")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 2
