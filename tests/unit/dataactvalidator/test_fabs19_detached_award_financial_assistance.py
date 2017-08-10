from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import CountryCode
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs19_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {"row_number", "legal_entity_country_code"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ LegalEntityCountryCode Field must contain a valid three character GENC Standard Edition 3.0 (Update 4)
        country code. """
    cc_1 = CountryCode(country_code="USA", country_name="United States")
    cc_2 = CountryCode(country_code="UKR", country_name="Ukraine")
    det_award = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="USA")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="uKr")

    errors = number_of_errors(_FILE, database, models=[cc_1, cc_2, det_award, det_award_2])
    assert errors == 0


def test_failure(database):
    """ LegalEntityCountryCode Field must contain a valid three character GENC Standard Edition 3.0 (Update 4)
        country code. """

    det_award = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="xyz")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="ABCD")

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2
