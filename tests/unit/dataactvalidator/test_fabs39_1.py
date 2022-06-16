from tests.unit.dataactcore.factories.staging import FABSFactory
from dataactcore.models.domainModels import States
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs39_1'


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
    fabs_1 = FABSFactory(place_of_performance_code='00*****', record_type=1, correction_delete_indicatr='')
    fabs_2 = FABSFactory(place_of_performance_code='00FORGN', record_type=1, correction_delete_indicatr=None)
    fabs_3 = FABSFactory(place_of_performance_code='00FORgN', record_type=2, correction_delete_indicatr='c')
    fabs_4 = FABSFactory(place_of_performance_code='NY*****', record_type=2, correction_delete_indicatr='C')
    fabs_5 = FABSFactory(place_of_performance_code='Ny*****', record_type=1, correction_delete_indicatr='')
    fabs_6 = FABSFactory(place_of_performance_code='NY**123', record_type=1, correction_delete_indicatr='')
    fabs_7 = FABSFactory(place_of_performance_code='NY12345', record_type=2, correction_delete_indicatr='')
    fabs_8 = FABSFactory(place_of_performance_code='NY1234R', record_type=2, correction_delete_indicatr='')
    fabs_9 = FABSFactory(place_of_performance_code='NY1234t', record_type=1, correction_delete_indicatr='')
    fabs_10 = FABSFactory(place_of_performance_code='NYTs123', record_type=2, correction_delete_indicatr='')
    # Ignored for record type 3
    fabs_11 = FABSFactory(place_of_performance_code='AB12345', record_type=3, correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    fabs_12 = FABSFactory(place_of_performance_code='001****', record_type=1, correction_delete_indicatr='d')
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7, fabs_8,
                                                       fabs_9, fabs_10, fabs_11, fabs_12, state_code])
    assert errors == 0


def test_failure(database):
    """ Test failure PrimaryPlaceOfPerformanceCode is a required field for aggregate and non-aggregate records
        (RecordType = 1 or 2), and must be in 00FORGN, 00*****, XX*****, XX**###, XX#####, or XX####R formats, where
        XX is a valid two-character state code, # are numerals, and 'R' is that letter.
    """

    state_code = States(state_code='NY')
    fabs_1 = FABSFactory(place_of_performance_code='001****', record_type=1, correction_delete_indicatr='')
    fabs_2 = FABSFactory(place_of_performance_code='NA*****', record_type=2, correction_delete_indicatr=None)
    fabs_3 = FABSFactory(place_of_performance_code='NA1234R', record_type=2, correction_delete_indicatr='c')
    fabs_4 = FABSFactory(place_of_performance_code='', record_type=1, correction_delete_indicatr='C')
    fabs_5 = FABSFactory(place_of_performance_code=None, record_type=2, correction_delete_indicatr='')
    # Invalid ppop format
    fabs_6 = FABSFactory(place_of_performance_code='NA1234X', record_type=2, correction_delete_indicatr='')
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, state_code])
    assert errors == 6
