from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs40_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'place_of_performance_code', 'record_type',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ PrimaryPlaceOfPerformanceCode for aggregate records (i.e., when RecordType = 1) must be in countywide
        (XX**###), statewide (XX*****), or foreign (00FORGN) formats.
    """

    fabs_1 = FABSFactory(place_of_performance_code='NY**123', record_type=1, correction_delete_indicatr='')
    fabs_2 = FABSFactory(place_of_performance_code='ny**987', record_type=1, correction_delete_indicatr=None)
    fabs_3 = FABSFactory(place_of_performance_code='NY*****', record_type=1, correction_delete_indicatr='c')
    fabs_4 = FABSFactory(place_of_performance_code='ny*****', record_type=1, correction_delete_indicatr='C')
    fabs_5 = FABSFactory(place_of_performance_code='00FORGN', record_type=1, correction_delete_indicatr='')
    fabs_6 = FABSFactory(place_of_performance_code='00forgn', record_type=1, correction_delete_indicatr='')

    # Ignore record type 2 and 3
    fabs_7 = FABSFactory(place_of_performance_code='00**333', record_type=2, correction_delete_indicatr='')
    fabs_8 = FABSFactory(place_of_performance_code='00**333', record_type=3, correction_delete_indicatr='')

    # Ignore correction delete indicator of D
    fabs_9 = FABSFactory(place_of_performance_code='00**333', record_type=1, correction_delete_indicatr='d')
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7, fabs_8,
                                                       fabs_9])
    assert errors == 0


def test_failure(database):
    """ Test failure for PrimaryPlaceOfPerformanceCode for aggregate records (i.e., when RecordType = 1)
        must be in countywide (XX**###), statewide (XX*****), or foreign (00FORGN) formats.
    """

    fabs_1 = FABSFactory(place_of_performance_code='00**333', record_type=1, correction_delete_indicatr='')
    fabs_2 = FABSFactory(place_of_performance_code='AB**33', record_type=1, correction_delete_indicatr=None)
    fabs_3 = FABSFactory(place_of_performance_code='00*****', record_type=1, correction_delete_indicatr='c')
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3])
    assert errors == 3
