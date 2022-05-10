from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs41_detached_award_financial_assistance_4'


def test_column_headers(database):
    expected_subset = {'row_number', 'place_of_performance_zip4a', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ When provided, PrimaryPlaceofPerformanceZIP+4 must be in the format #####, #########, #####-####,
        or "city-wide".
    """

    fabs_1 = FABSFactory(place_of_performance_zip4a='', correction_delete_indicatr='')
    fabs_2 = FABSFactory(place_of_performance_zip4a=None, correction_delete_indicatr=None)
    fabs_3 = FABSFactory(place_of_performance_zip4a='city-wide', correction_delete_indicatr='c')
    fabs_4 = FABSFactory(place_of_performance_zip4a='12345', correction_delete_indicatr='C')
    fabs_5 = FABSFactory(place_of_performance_zip4a='123456789', correction_delete_indicatr='')
    fabs_6 = FABSFactory(place_of_performance_zip4a='12345-6789', correction_delete_indicatr='')

    # Ignore correction delete indicator of D
    fabs_7 = FABSFactory(place_of_performance_zip4a='123456', correction_delete_indicatr='d')
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7])
    assert errors == 0


def test_failure(database):
    """ Test failure for when provided, PrimaryPlaceofPerformanceZIP+4 must be in the format #####, #########,
        #####-####, or "city-wide".
    """

    fabs_1 = FABSFactory(place_of_performance_zip4a='123456', correction_delete_indicatr='')
    fabs_2 = FABSFactory(place_of_performance_zip4a='12345_6789', correction_delete_indicatr=None)
    fabs_3 = FABSFactory(place_of_performance_zip4a='1234F', correction_delete_indicatr='c')
    fabs_4 = FABSFactory(place_of_performance_zip4a='1234567890', correction_delete_indicatr='C')
    fabs_5 = FABSFactory(place_of_performance_zip4a='1234', correction_delete_indicatr='')
    fabs_6 = FABSFactory(place_of_performance_zip4a='citywide', correction_delete_indicatr='')
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6])
    assert errors == 6
