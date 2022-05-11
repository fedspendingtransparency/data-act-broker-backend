from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs43_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'place_of_performance_zip4a', 'place_of_performance_congr',
                       'place_of_perform_country_c', 'record_type', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ For aggregate and non-aggregate records (RecordType = 1 or 2), with domestic place of performance
        (PrimaryPlaceOfPerformanceCountryCode = USA): if 9-digit PrimaryPlaceOfPerformanceZIP+4 is not provided,
        PrimaryPlaceOfPerformanceCongressionalDistrict must be provided.
    """

    fabs_1 = FABSFactory(place_of_performance_zip4a='', place_of_performance_congr='01',
                         place_of_perform_country_c='USA', record_type=1, correction_delete_indicatr='')
    fabs_2 = FABSFactory(place_of_performance_zip4a=None, place_of_performance_congr='01',
                         place_of_perform_country_c='USA', record_type=2, correction_delete_indicatr=None)
    fabs_3 = FABSFactory(place_of_performance_zip4a='123454321', place_of_performance_congr='',
                         place_of_perform_country_c='usa', record_type=1, correction_delete_indicatr='c')
    fabs_4 = FABSFactory(place_of_performance_zip4a='123454321', place_of_performance_congr=None,
                         place_of_perform_country_c='USA', record_type=2, correction_delete_indicatr='C')
    fabs_5 = FABSFactory(place_of_performance_zip4a='12345', place_of_performance_congr='02',
                         place_of_perform_country_c='USA', record_type=1, correction_delete_indicatr='')

    # Testing foreign places are ignored
    fabs_6 = FABSFactory(place_of_performance_zip4a='', place_of_performance_congr='', place_of_perform_country_c='uK',
                         record_type=1, correction_delete_indicatr='')
    fabs_7 = FABSFactory(place_of_performance_zip4a='city-wide', place_of_performance_congr='',
                         place_of_perform_country_c='uK', record_type=2, correction_delete_indicatr='')

    # Testing record type 3 entries are ignored
    fabs_8 = FABSFactory(place_of_performance_zip4a='', place_of_performance_congr='', place_of_perform_country_c='USA',
                         record_type=3, correction_delete_indicatr='')

    # Ignore correction delete indicator of D
    fabs_9 = FABSFactory(place_of_performance_zip4a='', place_of_performance_congr='', place_of_perform_country_c='USA',
                         record_type=1, correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7, fabs_8,
                                                       fabs_9])
    assert errors == 0


def test_failure(database):
    """ Test failure for aggregate and non-aggregate records (RecordType = 1 or 2), with domestic place of performance
        (PrimaryPlaceOfPerformanceCountryCode = USA): if 9-digit PrimaryPlaceOfPerformanceZIP+4 is not provided,
        PrimaryPlaceOfPerformanceCongressionalDistrict must be provided.
    """

    fabs_1 = FABSFactory(place_of_performance_zip4a='', place_of_performance_congr='',
                         place_of_perform_country_c='USA', record_type=1, correction_delete_indicatr='')
    fabs_2 = FABSFactory(place_of_performance_zip4a=None, place_of_performance_congr='',
                         place_of_perform_country_c='UsA', record_type=2, correction_delete_indicatr=None)
    fabs_3 = FABSFactory(place_of_performance_zip4a='', place_of_performance_congr=None,
                         place_of_perform_country_c='USA', record_type=1, correction_delete_indicatr='c')
    fabs_4 = FABSFactory(place_of_performance_zip4a=None, place_of_performance_congr=None,
                         place_of_perform_country_c='USA', record_type=2, correction_delete_indicatr='C')
    fabs_5 = FABSFactory(place_of_performance_zip4a='city-wide', place_of_performance_congr=None,
                         place_of_perform_country_c='USA', record_type=1, correction_delete_indicatr='')
    fabs_6 = FABSFactory(place_of_performance_zip4a='city-wide', place_of_performance_congr='',
                         place_of_perform_country_c='USA', record_type=2, correction_delete_indicatr='')
    fabs_7 = FABSFactory(place_of_performance_zip4a='12345', place_of_performance_congr='',
                         place_of_perform_country_c='usa', record_type=1, correction_delete_indicatr='')
    fabs_8 = FABSFactory(place_of_performance_zip4a='12345', place_of_performance_congr=None,
                         place_of_perform_country_c='USA', record_type=2, correction_delete_indicatr='')
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7, fabs_8])
    assert errors == 8
