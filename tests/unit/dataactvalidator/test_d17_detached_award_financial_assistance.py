from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd17_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {"row_number", "legal_entity_country_code", "legal_entity_foreign_posta"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ LegalEntityForeignPostalCode must be blank for domestic recipients
    when LegalEntityCountryCode is 'USA'. Foreign recipients don't affect success """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="Spain",
                                                          legal_entity_foreign_posta="12345")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="Peru",
                                                          legal_entity_foreign_posta="")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="Peru",
                                                          legal_entity_foreign_posta=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="USA",
                                                          legal_entity_foreign_posta=None)
    det_award_5 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="USA",
                                                          legal_entity_foreign_posta="")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ LegalEntityForeignPostalCode must be blank for domestic recipients when LegalEntityCountryCode is 'USA' """

    det_award = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="USA",
                                                        legal_entity_foreign_posta="12345")

    errors = number_of_errors(_FILE, database, models=[det_award])
    assert errors == 1
