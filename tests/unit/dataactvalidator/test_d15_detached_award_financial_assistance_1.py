from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd15_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {"row_number", "legal_entity_country_code", "legal_entity_foreign_city"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test success LegalEntityForeignCityName is required for foreign recipients
    (i.e., when LegalEntityCountryCode != USA) """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="Japan",
                                                          legal_entity_foreign_city="Tokyo")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="UK",
                                                          legal_entity_foreign_city="Manchester")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="USA",
                                                          legal_entity_foreign_city=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="USA",
                                                          legal_entity_foreign_city="")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_failure(database):
    """ Test failure LegalEntityForeignCityName is required for foreign recipients
    (i.e., when LegalEntityCountryCode != USA) """

    det_award = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="Japan",
                                                        legal_entity_foreign_city=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="Canada",
                                                          legal_entity_foreign_city="")

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2
