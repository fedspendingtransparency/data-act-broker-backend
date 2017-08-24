from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs14_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {"row_number", "record_type", "legal_entity_zip_last4"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test LegalEntityZIPLast4 must be blank for aggregate records (i.e., when RecordType = 1) record type 2 doesn't
        affect success """
    det_award = DetachedAwardFinancialAssistanceFactory(record_type=2, legal_entity_zip_last4="12345")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=2, legal_entity_zip_last4=None)
    det_award_null = DetachedAwardFinancialAssistanceFactory(record_type=1, legal_entity_zip_last4=None)
    det_award_null_2 = DetachedAwardFinancialAssistanceFactory(record_type=1, legal_entity_zip_last4='')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_null, det_award_null_2])
    assert errors == 0


def test_failure(database):
    """ Test failure when LegalEntityZIPLast4 isn't blank for aggregate records (i.e., when RecordType = 1) """

    det_award = DetachedAwardFinancialAssistanceFactory(record_type=1, legal_entity_zip_last4="Test")

    errors = number_of_errors(_FILE, database, models=[det_award])
    assert errors == 1
