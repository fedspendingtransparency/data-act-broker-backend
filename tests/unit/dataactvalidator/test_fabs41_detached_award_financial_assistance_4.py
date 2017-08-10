from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs41_detached_award_financial_assistance_4'


def test_column_headers(database):
    expected_subset = {"row_number", "place_of_performance_zip4a"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ When provided, PrimaryPlaceofPerformanceZIP+4 must be in the format #####, #########, or #####-#### """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a="")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a="12345")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a="123456789")
    det_award_5 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a="12345-6789")
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ Test failure for when provided, PrimaryPlaceofPerformanceZIP+4 must be in the format #####, #########,
        or #####-#### """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a="123456")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a='12345_6789')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a='1234F')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a='1234567890')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a='1234')
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 5
