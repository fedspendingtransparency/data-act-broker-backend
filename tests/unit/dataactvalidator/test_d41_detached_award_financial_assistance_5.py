from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from dataactcore.models.domainModels import Zips

_FILE = 'd41_detached_award_financial_assistance_5'


def test_column_headers(database):
    expected_subset = {"row_number", "place_of_performance_code", "place_of_performance_zip4a"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ The provided PrimaryPlaceofPerformanceZIP+4 must be in the state specified by PrimaryPlaceOfPerformanceCode.
        In this specific submission row, the first five digits are valid and located in the correct state, but the
        last 4 are invalid."""

    zips = Zips(zip5="12345", zip_last4="6789", state_abbreviation="NY")
    # ignored because no zip4
    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NY*****",
                                                          place_of_performance_zip4a="")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="Ny**123",
                                                          place_of_performance_zip4a=None)
    # valid 9 digit zip
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NY98765",
                                                          place_of_performance_zip4a="123456789")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="ny98765",
                                                          place_of_performance_zip4a="123456789")
    det_award_5 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="ny98765",
                                                          place_of_performance_zip4a="12345-6789")
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       zips])
    assert errors == 0

    # random wrong length zips and zips with '-' in the wrong place, formatting is checked in another rule
    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="ny10986",
                                                          place_of_performance_zip4a="12345678")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="ny10986",
                                                          place_of_performance_zip4a="1234567898")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="ny10986",
                                                          place_of_performance_zip4a="12345678-9")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="ny10986",
                                                          place_of_performance_zip4a="123-456789")
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, zips])
    assert errors == 0

    # invalid 5 digit zip - this should pass but is handled in d41_3
    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="ny10986",
                                                          place_of_performance_zip4a="12346")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NA*****",
                                                          place_of_performance_zip4a='12345')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NA*****",
                                                          place_of_performance_zip4a='12346-6789')
    # valid 5 digit zip
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="Ny**123",
                                                          place_of_performance_zip4a="12345")
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, zips])
    assert errors == 0


def test_failure(database):
    """ Test failure for when the provided PrimaryPlaceofPerformanceZIP+4 must be in the state specified by
        PrimaryPlaceOfPerformanceCode. In this specific submission row, the first five digits are valid and located
        in the correct state, but the last 4 are invalid."""
    zips = Zips(zip5="12345", zip_last4="6789", state_abbreviation="NY")

    # invalid 9 digit zip - first 5 digits good
    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="ny10986",
                                                          place_of_performance_zip4a="123456788")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NY*****",
                                                          place_of_performance_zip4a='123456788')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="Ny**123",
                                                          place_of_performance_zip4a='12345-6788')
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, zips])
    assert errors == 3
