from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd35_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {"row_number", "legal_entity_zip_last4", "legal_entity_congressional"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ LegalEntityZIPLast4 or LegalEntityCongressionalDistrict must be provided. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4="1234",
                                                          legal_entity_congressional=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4=None,
                                                          legal_entity_congressional="1234")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4="1234",
                                                          legal_entity_congressional="1234")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3])
    assert errors == 0


def test_failure(database):
    """ LegalEntityZIPLast4 or LegalEntityCongressionalDistrict must be provided. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4=None,
                                                          legal_entity_congressional=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4='',
                                                          legal_entity_congressional='')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 2
