from tests.unit.dataactcore.factories.staging import FABSFactory
from dataactcore.models.domainModels import CountyCode
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs40_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'place_of_performance_code', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ PrimaryPlaceOfPerformanceCode last three digits must be a valid county code when format is XX**###. """

    county_code = CountyCode(county_number='123', state_code='NY')
    fabs_1 = FABSFactory(place_of_performance_code='NY*****', correction_delete_indicatr='')
    fabs_2 = FABSFactory(place_of_performance_code='00FO333', correction_delete_indicatr=None)
    fabs_3 = FABSFactory(place_of_performance_code='NY**123', correction_delete_indicatr='c')
    fabs_4 = FABSFactory(place_of_performance_code='Ny**123', correction_delete_indicatr='C')
    # Ignore correction delete indicator of D
    fabs_5 = FABSFactory(place_of_performance_code='00**333', correction_delete_indicatr='d')
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, county_code])
    assert errors == 0


def test_failure(database):
    """ Test failure for PrimaryPlaceOfPerformanceCode last three digits must be a valid county code when
        format is XX**###.
    """

    county_code = CountyCode(county_number='123', state_code='NY')
    fabs_1 = FABSFactory(place_of_performance_code='00**333', correction_delete_indicatr='')
    fabs_2 = FABSFactory(place_of_performance_code='00**33', correction_delete_indicatr=None)
    fabs_3 = FABSFactory(place_of_performance_code='Ny**124', correction_delete_indicatr='c')
    fabs_4 = FABSFactory(place_of_performance_code='NA**123', correction_delete_indicatr='C')
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, county_code])
    assert errors == 4
