from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs35_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {"row_number", "legal_entity_zip_last4"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ When provided, LegalEntityZIPLast4 must be in the format ####. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4="1234")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4="")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3])
    assert errors == 0


def test_failure(database):
    """ When provided, LegalEntityZIPLast4 must be in the format ####. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4="123")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4="12345")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4="ABCD")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4="123D")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 4
