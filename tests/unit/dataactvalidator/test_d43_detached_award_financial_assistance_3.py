from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from dataactcore.models.domainModels import Zips

_FILE = 'd43_detached_award_financial_assistance_3'


def test_column_headers(database):
    expected_subset = {"row_number", "place_of_performance_code", "place_of_performance_congr"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test PrimaryPlaceOfPerformanceCongressionalDistrict exists in the state indicated by the
        PrimaryPlaceOfPerformanceCode or is 90 in a state with multiple districts or when PrimaryPlaceOfPerformanceCode
        is 00***** """
    zips1 = Zips(zip5='12345', zip_last4='6789', congressional_district_no="01", state_abbreviation="NY")
    zips2 = Zips(zip5='98765', zip_last4='4321', congressional_district_no="02", state_abbreviation="NY")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NY12345",
                                                          place_of_performance_congr="01")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="ny*****",
                                                          place_of_performance_congr="02")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="Ny12345",
                                                          place_of_performance_congr="90")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="00*****",
                                                          place_of_performance_congr="90")
    det_award_5 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NY12345",
                                                          place_of_performance_congr="")
    det_award_6 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="NY12345",
                                                          place_of_performance_congr=None)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6, zips1, zips2])
    assert errors == 0


def test_failure(database):
    """ Test failure PrimaryPlaceOfPerformanceCongressionalDistrict exists in the state indicated by the
        PrimaryPlaceOfPerformanceCode or is 90 in a state with multiple districts or when PrimaryPlaceOfPerformanceCode
        is 00***** """
    zips1 = Zips(zip5='12345', zip_last4='6789', congressional_district_no="01", state_abbreviation="NY")
    zips2 = Zips(zip5='98765', zip_last4='4321', congressional_district_no="02", state_abbreviation="NY")
    zips3 = Zips(zip5='12345', zip_last4='4321', congressional_district_no="01", state_abbreviation="PA")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="nY12345",
                                                          place_of_performance_congr="03")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="PA12345",
                                                          place_of_performance_congr="02")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="PA**345",
                                                          place_of_performance_congr="90")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(place_of_performance_code="00*****",
                                                          place_of_performance_congr="01")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, zips1, zips2,
                                                       zips3])
    assert errors == 4
