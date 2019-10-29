from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs39_detached_award_financial_assistance_4'


def test_column_headers(database):
    expected_subset = {'row_number', 'record_type', 'place_of_performance_code',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ PrimaryPlaceOfPerformanceCode must be blank for PII-redacted non-aggregate records (i.e., RecordType = 3). """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='NY12345', record_type=1,
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='ny98765', record_type=2,
                                                          correction_delete_indicatr='c')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code=None, record_type=3,
                                                          correction_delete_indicatr=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='', record_type=3,
                                                          correction_delete_indicatr='C')
    # Ignore correction delete indicator of D
    det_award_5 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='00FORGN', record_type=3,
                                                          correction_delete_indicatr='d')
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ Test failure for PrimaryPlaceOfPerformanceCode must be blank for PII-redacted non-aggregate records
        (i.e., RecordType = 3).
    """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='00FORGN', record_type=3,
                                                          correction_delete_indicatr='')
    errors = number_of_errors(_FILE, database, models=[det_award_1])
    assert errors == 1
