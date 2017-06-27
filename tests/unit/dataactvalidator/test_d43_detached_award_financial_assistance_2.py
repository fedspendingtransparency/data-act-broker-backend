from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from dataactcore.models.domainModels import Zips

_FILE = 'd43_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {"row_number", "place_of_performance_zip4a", "place_of_performance_congr"}
    actual = set(query_columns(_FILE, database))
    print(actual)
    assert expected_subset == actual


def test_success(database):
    """ When provided, PrimaryPlaceofPerformanceZIP+4, placeOfPerformanceCongressCode must be in the state specified
        by PrimaryPlaceOfPerformanceCode. Ignore cases where zip4 isn't provided """

    zips1 = Zips(zip5="12345", zip_last4="6789", state_abbreviation="NY", congressional_district_no="04")
    zips2 = Zips(zip5="12345", zip_last4="6780", state_abbreviation="NY", congressional_district_no="03")
    zips3 = Zips(zip5="12345", zip_last4="6781", state_abbreviation="NY", congressional_district_no="02")
    zips4 = Zips(zip5="12345", zip_last4="6782", state_abbreviation="NY", congressional_district_no="01")
    # ignored because no zip4
    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a="",
                                                          place_of_performance_congr="01")

    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a=None,
                                                          place_of_performance_congr="03")
    # valid 5 digit zip
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a="12345",
                                                          place_of_performance_congr="04")

    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a="12345",
                                                          place_of_performance_congr="01")
    # valid 9 digit zip
    det_award_5 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a="123456789",
                                                          place_of_performance_congr="04")

    det_award_6 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a="123456780",
                                                          place_of_performance_congr="03")

    det_award_7 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a="12345-6781",
                                                          place_of_performance_congr="02")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6, det_award_7, zips1, zips2, zips3, zips4])
    assert errors == 0


def test_failure(database):
    """ Test failure for PrimaryPlaceOfPerformanceCongressCode not in PrimaryPlaceOfPerformanceCode or not provided
        """

    zips1 = Zips(zip5="12345", zip_last4="6789", state_abbreviation="NY", congressional_district_no="04")
    zips2 = Zips(zip5="12345", zip_last4="6780", state_abbreviation="NY", congressional_district_no="03")
    zips3 = Zips(zip5="12345", zip_last4="6781", state_abbreviation="NY", congressional_district_no="02")
    zips4 = Zips(zip5="12345", zip_last4="6782", state_abbreviation="NY", congressional_district_no="01")
    # invalid 5 digit zip
    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a="12346",
                                                          place_of_performance_congr="05")

    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a='12345',
                                                          place_of_performance_congr="06")

    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a='12345',
                                                          place_of_performance_congr="06")
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, zips1, zips2,
                                                       zips3, zips4])
    assert errors == 3

    # invalid 9 digit zip
    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a="123456780",
                                                          place_of_performance_congr="01")

    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a='123456789',
                                                          place_of_performance_congr="02")

    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_zip4a='12345-6781',
                                                          place_of_performance_congr="03")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, zips1, zips2,
                                                       zips3, zips4])
    assert errors == 3
