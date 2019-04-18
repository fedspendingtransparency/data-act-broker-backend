from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import CountryCode
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs24_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {"row_number", "record_type", "place_of_perform_country_c"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ PrimaryPlaceOfPerformanceCountryCode must contain a valid three character GENC country code for aggregate or
        non-aggregate records (RecordType = 1 or 2). U.S. Territories and Freely Associated States must be submitted
        with country code = USA and their state/territory code; they cannot be submitted with their GENC country code.
    """
    cc_1 = CountryCode(country_code="USA", country_name="United States", territory_free_state=False)
    cc_2 = CountryCode(country_code="UKR", country_name="Ukraine", territory_free_state=False)
    det_award = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c="USA", record_type=1)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c="uKr", record_type=2)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c="abc", record_type=3)

    errors = number_of_errors(_FILE, database, models=[cc_1, cc_2, det_award, det_award_2, det_award_3])
    assert errors == 0


def test_failure(database):
    """ PrimaryPlaceOfPerformanceCountryCode must contain a valid three character GENC country code for aggregate or
        non-aggregate records (RecordType = 1 or 2). U.S. Territories and Freely Associated States must be submitted
        with country code = USA and their state/territory code; they cannot be submitted with their GENC country code.
    """

    cc_1 = CountryCode(country_code="ASM", country_name="AMERICAN SAMOA", territory_free_state=True)
    det_award = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c="xyz", record_type=1)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c="ABCD", record_type=2)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c="", record_type=2)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c="ASM", record_type=1)

    errors = number_of_errors(_FILE, database, models=[cc_1, det_award, det_award_2, det_award_3, det_award_4])
    assert errors == 4
