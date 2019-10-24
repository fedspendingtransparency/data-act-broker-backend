from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs10_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'record_type', 'legal_entity_address_line1', 'uniqueid_afa_generated_unique'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test LegalEntityAddressLine1 is required for non-aggregate records (i.e., when RecordType = 2) """
    det_award = DetachedAwardFinancialAssistanceFactory(record_type=2, legal_entity_address_line1='12345 Test Address',
                                                        correction_delete_indicatr=None)
    det_award_null = DetachedAwardFinancialAssistanceFactory(record_type=1, legal_entity_address_line1=None,
                                                             correction_delete_indicatr='C')
    # Ignore correction delete indicator of D
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=2, legal_entity_address_line1=None,
                                                          correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_null, det_award_2])
    assert errors == 0


def test_failure(database):
    """ Test failure when LegalEntityAddressLine1 is absent for non-aggregate records """

    det_award = DetachedAwardFinancialAssistanceFactory(record_type=2, legal_entity_address_line1=None,
                                                        correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[det_award])
    assert errors == 1
