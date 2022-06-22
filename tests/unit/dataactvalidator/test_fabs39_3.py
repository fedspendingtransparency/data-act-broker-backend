from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs39_3'


def test_column_headers(database):
    expected_subset = {'row_number', 'record_type', 'place_of_performance_code', 'place_of_performance_zip4a',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ For aggregate or non-aggregate records (RecordType = 1 or 2): PrimaryPlaceofPerformanceZIP+4 must not be
        provided for any format of PrimaryPlaceOfPerformanceCode other than XX#####, XXTS###, XX####T, or XX####R.
    """

    # place_of_performance_code = None should technically be a failure based on the rule, but because it is
    # tested elsewhere we want to ignore it.
    fabs_1 = FABSFactory(place_of_performance_code='NY12345', place_of_performance_zip4a='1234', record_type=1,
                         correction_delete_indicatr='')
    fabs_2 = FABSFactory(place_of_performance_code='ny98765', place_of_performance_zip4a='4312', record_type=1,
                         correction_delete_indicatr=None)
    fabs_3 = FABSFactory(place_of_performance_code='ny9876R', place_of_performance_zip4a='4312', record_type=1,
                         correction_delete_indicatr='c')
    fabs_4 = FABSFactory(place_of_performance_code='ny9876t', place_of_performance_zip4a='4312', record_type=1,
                         correction_delete_indicatr='c')
    fabs_5 = FABSFactory(place_of_performance_code='nytS987', place_of_performance_zip4a='4312', record_type=1,
                         correction_delete_indicatr='c')
    fabs_6 = FABSFactory(place_of_performance_code=None, place_of_performance_zip4a='4312', record_type=2,
                         correction_delete_indicatr='C')
    fabs_7 = FABSFactory(place_of_performance_code='ny**987', place_of_performance_zip4a=None, record_type=2,
                         correction_delete_indicatr='')
    fabs_8 = FABSFactory(place_of_performance_code='00*****', place_of_performance_zip4a='', record_type=1,
                         correction_delete_indicatr='')
    fabs_9 = FABSFactory(place_of_performance_code='00*****', place_of_performance_zip4a='abcde', record_type=3,
                         correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    fabs_10 = FABSFactory(place_of_performance_code='00FORGN', place_of_performance_zip4a='1234', record_type=1,
                          correction_delete_indicatr='d')
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7, fabs_8,
                                                       fabs_9, fabs_10])
    assert errors == 0


def test_failure(database):
    """ Test failure for aggregate or non-aggregate records (RecordType = 1 or 2): PrimaryPlaceofPerformanceZIP+4 must
        not be provided for any format of PrimaryPlaceOfPerformanceCode other than XX#####, XXTS###, XX####T, or
        XX####R.
    """

    fabs_1 = FABSFactory(place_of_performance_code='00FORGN', place_of_performance_zip4a='1234', record_type=1,
                         correction_delete_indicatr='')
    fabs_2 = FABSFactory(place_of_performance_code='00*****', place_of_performance_zip4a='4312', record_type=1,
                         correction_delete_indicatr=None)
    fabs_3 = FABSFactory(place_of_performance_code='ny**987', place_of_performance_zip4a='4312', record_type=2,
                         correction_delete_indicatr='c')
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3])
    assert errors == 3
