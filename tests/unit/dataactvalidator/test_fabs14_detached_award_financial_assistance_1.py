from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs14_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'record_type', 'legal_entity_zip_last4', 'uniqueid_afa_generated_unique'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test LegalEntityZIPLast4 must be blank for aggregate and PII-redacted non-aggregate records
        (i.e., when RecordType = 1 or 3). Record type 2 doesn't affect success
    """
    det_award = DetachedAwardFinancialAssistanceFactory(record_type=2, legal_entity_zip_last4='12345',
                                                        correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=2, legal_entity_zip_last4=None,
                                                          correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(record_type=1, legal_entity_zip_last4=None,
                                                          correction_delete_indicatr='c')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(record_type=1, legal_entity_zip_last4='',
                                                          correction_delete_indicatr='C')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(record_type=3, legal_entity_zip_last4=None,
                                                          correction_delete_indicatr='')
    det_award_6 = DetachedAwardFinancialAssistanceFactory(record_type=3, legal_entity_zip_last4='',
                                                          correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    det_award_7 = DetachedAwardFinancialAssistanceFactory(record_type=3, legal_entity_zip_last4='Test',
                                                          correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6, det_award_7])
    assert errors == 0


def test_failure(database):
    """ Test failure when LegalEntityZIPLast4 isn't blank for aggregate and PII-redacted non-aggregate records
        (i.e., when RecordType = 1 or 3)
    """

    det_award = DetachedAwardFinancialAssistanceFactory(record_type=1, legal_entity_zip_last4='Test',
                                                        correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=3, legal_entity_zip_last4='Test',
                                                          correction_delete_indicatr='c')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2
