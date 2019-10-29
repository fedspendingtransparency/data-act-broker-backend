from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs40_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'place_of_performance_code', 'record_type',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ PrimaryPlaceOfPerformanceCode for aggregate records (i.e., when RecordType = 1) must be in countywide
        (XX**###), statewide (XX*****), or foreign (00FORGN) formats.
    """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='NY**123', record_type=1,
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='ny**987', record_type=1,
                                                          correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='NY*****', record_type=1,
                                                          correction_delete_indicatr='c')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='ny*****', record_type=1,
                                                          correction_delete_indicatr='C')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='00FORGN', record_type=1,
                                                          correction_delete_indicatr='')
    det_award_6 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='00forgn', record_type=1,
                                                          correction_delete_indicatr='')
    # Ignore record type 2 and 3
    det_award_7 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='00**333', record_type=2,
                                                          correction_delete_indicatr='')
    det_award_8 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='00**333', record_type=3,
                                                          correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    det_award_9 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='00**333', record_type=1,
                                                          correction_delete_indicatr='d')
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4,
                                                       det_award_5, det_award_6, det_award_7, det_award_8,
                                                       det_award_9])
    assert errors == 0


def test_failure(database):
    """ Test failure for PrimaryPlaceOfPerformanceCode for aggregate records (i.e., when RecordType = 1)
        must be in countywide (XX**###), statewide (XX*****), or foreign (00FORGN) formats.
    """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='00**333', record_type=1,
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='AB**33', record_type=1,
                                                          correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='00*****', record_type=1,
                                                          correction_delete_indicatr='c')
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3])
    assert errors == 3
