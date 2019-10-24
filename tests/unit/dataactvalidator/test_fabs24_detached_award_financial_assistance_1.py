from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs24_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'record_type', 'place_of_perform_country_c', 'uniqueid_afa_generated_unique'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ PrimaryPlaceOfPerformanceCountryCode must be blank for record type 3. """
    det_award = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c='USA', record_type=1,
                                                        correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c='uKr', record_type=2,
                                                          correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c='', record_type=3,
                                                          correction_delete_indicatr='c')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c=None, record_type=3,
                                                          correction_delete_indicatr='C')
    # Ignore correction delete indicator of D
    det_award_5 = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c='USA', record_type=3,
                                                          correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ PrimaryPlaceOfPerformanceCountryCode must be blank for record type 3. """

    det_award = DetachedAwardFinancialAssistanceFactory(place_of_perform_country_c='USA', record_type=3,
                                                        correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[det_award])
    assert errors == 1
