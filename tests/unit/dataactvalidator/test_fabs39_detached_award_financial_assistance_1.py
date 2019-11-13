from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import States
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs39_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'record_type', 'place_of_performance_code',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ PrimaryPlaceOfPerformanceCode is a required field for aggregate and non-aggregate records (RecordType = 1 or 2),
        and must be in 00FORGN, 00*****, XX*****, XX**###, XX#####, or XX####R formats, where XX is a valid
        two-character state code, # are numerals, and 'R' is that letter.
    """

    state_code = States(state_code='NY')
    # Required for these values
    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='00*****', record_type=1,
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='00FORGN', record_type=1,
                                                          correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='00FORgN', record_type=2,
                                                          correction_delete_indicatr='c')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='NY*****', record_type=2,
                                                          correction_delete_indicatr='C')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='Ny*****', record_type=1,
                                                          correction_delete_indicatr='')
    det_award_6 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='NY**123', record_type=1,
                                                          correction_delete_indicatr='')
    det_award_7 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='NY12345', record_type=2,
                                                          correction_delete_indicatr='')
    det_award_8 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='NY1234R', record_type=2,
                                                          correction_delete_indicatr='')
    # Ignored for record type 3
    det_award_9 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='AB12345', record_type=3,
                                                          correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    det_award_10 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='001****', record_type=1,
                                                           correction_delete_indicatr='d')
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6, det_award_7, det_award_8, det_award_9, det_award_10,
                                                       state_code])
    assert errors == 0


def test_failure(database):
    """ Test failure PrimaryPlaceOfPerformanceCode is a required field for aggregate and non-aggregate records
        (RecordType = 1 or 2), and must be in 00FORGN, 00*****, XX*****, XX**###, XX#####, or XX####R formats, where
        XX is a valid two-character state code, # are numerals, and 'R' is that letter.
    """

    state_code = States(state_code='NY')
    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='001****', record_type=1,
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='NA*****', record_type=2,
                                                          correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='NA1234R', record_type=2,
                                                          correction_delete_indicatr='c')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='', record_type=1,
                                                          correction_delete_indicatr='C')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code=None, record_type=2,
                                                          correction_delete_indicatr='')
    # Invalid ppop format
    det_award_6 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='NA1234X', record_type=2,
                                                          correction_delete_indicatr='')
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6, state_code])
    assert errors == 6
