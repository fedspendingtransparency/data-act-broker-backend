from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs12_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {"row_number", "record_type", "legal_entity_address_line3"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test LegalEntityAddressLine3 is optional, but must be blank for aggregate records (i.e., when RecordType = 1)
        record_type 2 doesn't affect success """
    det_award = DetachedAwardFinancialAssistanceFactory(record_type=2, legal_entity_address_line3="12345 Test Address")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=2, legal_entity_address_line3=None)
    det_award_null = DetachedAwardFinancialAssistanceFactory(record_type=1, legal_entity_address_line3=None)
    det_award_null_2 = DetachedAwardFinancialAssistanceFactory(record_type=1, legal_entity_address_line3='')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_null, det_award_null_2])
    assert errors == 0


def test_failure(database):
    """ Test failure when LegalEntityAddressLine3 isn't blank for aggregate records (i.e., when RecordType = 1) """

    det_award = DetachedAwardFinancialAssistanceFactory(record_type=1, legal_entity_address_line3="Test")

    errors = number_of_errors(_FILE, database, models=[det_award])
    assert errors == 1
