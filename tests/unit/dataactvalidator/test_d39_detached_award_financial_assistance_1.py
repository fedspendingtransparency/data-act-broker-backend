from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import Zips
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd39_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {"row_number", "place_of_performance_code"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ 00***** is a valid PrimaryPlaceOfPerformanceCode value and indicates a multi-state project.
        00FORGN indicates that the place of performance is in a foreign country (allow it to pass, don't test).
        If neither of the above, PrimaryPlaceOfPerformanceCode must start with valid 2 character state abbreviation
    """

    zip_code = Zips(zip5="12345", state_abbreviation="NY")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="00*****")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="00FORGN")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="00FORgN")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NY*****")
    det_award_5 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="Ny*****")
    det_award_6 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NY**ABC")
    det_award_7 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NY12345")
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6, det_award_7, zip_code])
    assert errors == 0


def test_failure(database):
    """ Test for failure that PrimaryPlaceOfPerformanceCode must start with 2 character state abbreviation """

    zip_code = Zips(zip5="12345", state_abbreviation="NY")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="001****")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NA*****")
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, zip_code])
    assert errors == 2
