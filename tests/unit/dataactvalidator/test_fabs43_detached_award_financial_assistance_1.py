from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs43_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'place_of_perform_country_c', 'place_of_performance_congr',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ If PrimaryPlaceOfPerformanceCode is not USA, Congressional District must be blank """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c='Nk',
                                                          place_of_performance_congr='',
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c='CA',
                                                          place_of_performance_congr=None,
                                                          correction_delete_indicatr='c')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c='Usa',
                                                          place_of_performance_congr='',
                                                          correction_delete_indicatr=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c='USA',
                                                          place_of_performance_congr='12',
                                                          correction_delete_indicatr='C')
    # Ignore correction delete indicator of D
    det_award_5 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c='Nk',
                                                          place_of_performance_congr='12',
                                                          correction_delete_indicatr='d')
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ Test failure for if PrimaryPlaceOfPerformanceCode is not USA, Congressional District must be blank """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c='Nk',
                                                          place_of_performance_congr='12',
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c='CA',
                                                          place_of_performance_congr='32',
                                                          correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c='Mx',
                                                          place_of_performance_congr='09',
                                                          correction_delete_indicatr='c')
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3])
    assert errors == 3
