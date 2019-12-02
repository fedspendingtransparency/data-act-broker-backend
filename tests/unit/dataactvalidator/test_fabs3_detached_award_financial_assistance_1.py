from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs3_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'action_type', 'record_type', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests success for when Action type is required for non-aggregate and PII-redacted non-aggregate records
        (i.e., when RecordType = 2 or 3).
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(record_type=2, action_type='B', correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=3, action_type='B',
                                                          correction_delete_indicatr=None)
    # Ignore record type 1
    det_award_3 = DetachedAwardFinancialAssistanceFactory(record_type=1, action_type='', correction_delete_indicatr='c')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(record_type=1, action_type=None,
                                                          correction_delete_indicatr='C')
    # Ignore correction delete indicator of D
    det_award_5 = DetachedAwardFinancialAssistanceFactory(record_type=3, action_type=None,
                                                          correction_delete_indicatr='D')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ Tests that failure for when Action type is required for non-aggregate and PII-redacted non-aggregate records
        (i.e., when RecordType = 2 or 3).
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(record_type=2, action_type='', correction_delete_indicatr='c')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=2, action_type=None,
                                                          correction_delete_indicatr='')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(record_type=3, action_type=None,
                                                          correction_delete_indicatr=None)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3])
    assert errors == 3
