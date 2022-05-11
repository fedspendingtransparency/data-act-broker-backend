from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs41_detached_award_financial_assistance_8'


def test_column_headers(database):
    expected_subset = {'row_number', 'place_of_performance_code', 'place_of_performance_zip4a',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ When PrimaryPlaceOfPerformanceCode is in XX##### or XX####R format, PrimaryPlaceOfPerformanceZIP+4 must not be
        blank (containing either a zip code or 'city-wide'). """
    fabs = FABSFactory(place_of_performance_code='Ny12345', place_of_performance_zip4a='not blank')
    fabs_2 = FABSFactory(place_of_performance_code='nY1234R', place_of_performance_zip4a='12345')
    fabs_3 = FABSFactory(place_of_performance_code='wrong format', place_of_performance_zip4a='city-wide')
    fabs_4 = FABSFactory(place_of_performance_code='wrong format', place_of_performance_zip4a=None)

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """ Test fail When PrimaryPlaceOfPerformanceCode is in XX##### or XX####R format, PrimaryPlaceOfPerformanceZIP+4
        must not be blank (containing either a zip code or 'city-wide'). """

    fabs = FABSFactory(place_of_performance_code='Ny12345', place_of_performance_zip4a=None)
    fabs_2 = FABSFactory(place_of_performance_code='nY1234R', place_of_performance_zip4a=None)

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2])
    assert errors == 2
