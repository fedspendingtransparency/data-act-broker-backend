from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs42_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'place_of_performance_forei', 'place_of_perform_country_c', 'record_type',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test PrimaryPlaceOfPerformanceForeignLocationDescription is required for foreign places of performance
        (i.e., when PrimaryPlaceOfPerformanceCountryCode does not equal USA) for record type 2. This test shouldn't
       care about content when country_code is USA (that is for another validation).
    """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_forei='description',
                                                          place_of_perform_country_c='UK', record_type=2,
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_forei='description',
                                                          place_of_perform_country_c='USA', record_type=2,
                                                          correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_forei=None,
                                                          place_of_perform_country_c='USA', record_type=2,
                                                          correction_delete_indicatr='c')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_forei='',
                                                          place_of_perform_country_c='UsA', record_type=2,
                                                          correction_delete_indicatr='C')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(place_of_performance_forei='',
                                                          place_of_perform_country_c='UK', record_type=1,
                                                          correction_delete_indicatr='')
    det_award_6 = DetachedAwardFinancialAssistanceFactory(place_of_performance_forei=None,
                                                          place_of_perform_country_c='UK', record_type=1,
                                                          correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    det_award_7 = DetachedAwardFinancialAssistanceFactory(place_of_performance_forei='',
                                                          place_of_perform_country_c='UK', record_type=2,
                                                          correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4,
                                                       det_award_5, det_award_6, det_award_7])
    assert errors == 0


def test_failure(database):
    """ Test failure PrimaryPlaceOfPerformanceForeignLocationDescription is required for foreign places of performance
        (i.e., when PrimaryPlaceOfPerformanceCountryCode does not equal USA) for record type 2.
    """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_forei='',
                                                          place_of_perform_country_c='UK', record_type=2,
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_forei=None,
                                                          place_of_perform_country_c='UK', record_type=2,
                                                          correction_delete_indicatr='c')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 2
