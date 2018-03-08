from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import CountryCode
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs24_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {"row_number", "record_type", "place_of_perform_country_c"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ PrimaryPlaceOfPerformanceCountryCode must contain a valid three character GENC Standard Edition 3.0 (Update 4)
        country code for record type 1 and 2. """
    cc_1 = CountryCode(country_code="USA", country_name="United States")
    cc_2 = CountryCode(country_code="UKR", country_name="Ukraine")
    det_award = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c="USA", record_type=1)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c="uKr", record_type=2)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c="abc", record_type=3)

    errors = number_of_errors(_FILE, database, models=[cc_1, cc_2, det_award, det_award_2, det_award_3])
    assert errors == 0


def test_failure(database):
    """ PrimaryPlaceOfPerformanceCountryCode must contain a valid three character GENC Standard Edition 3.0 (Update 4)
        country code for record type 1 and 2. """

    det_award = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c="xyz", record_type=1)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c="ABCD", record_type=2)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c="", record_type=2)

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3])
    assert errors == 3
