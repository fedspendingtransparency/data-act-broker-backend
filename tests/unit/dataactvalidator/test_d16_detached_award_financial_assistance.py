from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd16_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {"row_number", "legal_entity_country_code", "legal_entity_foreign_provi"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ LegalEntityForeignProvinceName must be blank for foreign recipients
    (i.e., when LegalEntityCountryCode = USA). Foreign reign recipients don't affect success."""

    det_award = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="Japan",
                                                        legal_entity_foreign_provi="Yamashiro")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="UK",
                                                          legal_entity_foreign_provi="")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="UK",
                                                          legal_entity_foreign_provi=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="USA",
                                                          legal_entity_foreign_provi=None)
    det_award_5 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="USA",
                                                          legal_entity_foreign_provi="")

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ Test failure LegalEntityForeignProvinceName must be blank for foreign recipients
    (i.e., when LegalEntityCountryCode = USA) """

    det_award = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="USA",
                                                        legal_entity_foreign_provi="Test")

    errors = number_of_errors(_FILE, database, models=[det_award])
    assert errors == 1
