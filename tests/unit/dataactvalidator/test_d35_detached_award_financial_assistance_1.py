from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd35_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {"row_number", "legal_entity_zip_last4", "record_type", "legal_entity_country_code"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ LegalEntityZIPLast4 should be provided. No warning when RecordType = 1 or LegalEntityCountryCode != USA. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4="1234", record_type=2,
                                                          legal_entity_country_code="usa")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4=None, record_type=1,
                                                          legal_entity_country_code="USA")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4="", record_type=1,
                                                          legal_entity_country_code="USA")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4="", record_type=2,
                                                          legal_entity_country_code="uk")
    det_award_5 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4=None, record_type=2,
                                                          legal_entity_country_code="uk")
    det_award_6 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4="1234", record_type=1,
                                                          legal_entity_country_code="uk")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6])
    assert errors == 0


def test_failure(database):
    """ LegalEntityZIPLast4 should be provided. No warning when RecordType = 1 or LegalEntityCountryCode != USA. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4=None, record_type=2,
                                                          legal_entity_country_code="USA")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4='', record_type=2,
                                                          legal_entity_country_code="UsA")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 2
