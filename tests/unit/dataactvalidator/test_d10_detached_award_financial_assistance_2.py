from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd10_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {"row_number", "record_type", "legal_entity_address_line1"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test LegalEntityAddressLine1 must be blank for aggregate records (i.e., when RecordType = 1) """
    det_award = DetachedAwardFinancialAssistanceFactory(record_type=2, legal_entity_address_line1="12345 Test Address")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=1, legal_entity_address_line1="")
    det_award_null = DetachedAwardFinancialAssistanceFactory(record_type=1, legal_entity_address_line1=None)

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_null])
    assert errors == 0


def test_failure(database):
    """ Test failure when LegalEntityAddressLine1 is not blank for aggregate records """

    det_award = DetachedAwardFinancialAssistanceFactory(record_type=1, legal_entity_address_line1="12345 Test Address")

    errors = number_of_errors(_FILE, database, models=[det_award])
    assert errors == 1
