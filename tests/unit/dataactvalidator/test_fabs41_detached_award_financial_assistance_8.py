from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs41_detached_award_financial_assistance_8'


def test_column_headers(database):
    expected_subset = {'row_number', 'place_of_performance_code', 'place_of_performance_zip4a',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ When PrimaryPlaceOfPerformanceCode is in XX##### or XX####R format, PrimaryPlaceOfPerformanceZIP+4 must not be
        blank (containing either a zip code or ‘city-wide’). """
    det_award = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='Ny12345',
                                                        place_of_performance_zip4a='not blank')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='nY1234R',
                                                          place_of_performance_zip4a='12345')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='wrong format',
                                                          place_of_performance_zip4a='city-wide')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='wrong format',
                                                          place_of_performance_zip4a=None)

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_failure(database):
    """ Test fail When PrimaryPlaceOfPerformanceCode is in XX##### or XX####R format, PrimaryPlaceOfPerformanceZIP+4
        must not be blank (containing either a zip code or ‘city-wide’). """

    det_award = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='Ny12345',
                                                        place_of_performance_zip4a=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code='nY1234R',
                                                          place_of_performance_zip4a=None)

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2
