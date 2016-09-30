from random import randint, choice
from string import ascii_uppercase, ascii_lowercase, digits
from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'a1_appropriations'

def test_column_headers(database):
    expected_subset = {"row_number", "allocation_transfer_agency", "agency_identifier", "beginning_period_of_availa",
        "ending_period_of_availabil", "availability_type_code", "main_account_code", "sub_account_code"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset <= actual

def test_success(database):
    """ Test that TAS values can be found, and null matches work correctly"""
    # Create a 12 character random piid
    tas = TASFactory()
    tas_null = TASFactory(allocation_transfer_agency = None)
    approp = AppropriationFactory(allocation_transfer_agency = tas.allocation_transfer_agency,
                                  agency_identifier = tas.agency_identifier,
                                  beginning_period_of_availa = tas.beginning_period_of_availability,
                                  ending_period_of_availabil = tas.ending_period_of_availability,
                                  availability_type_code = tas.availability_type_code,
                                  main_account_code = tas.main_account_code,
                                  sub_account_code = tas.sub_account_code)
    approp_null = AppropriationFactory(allocation_transfer_agency = None,
                                       agency_identifier = tas.agency_identifier,
                                       beginning_period_of_availa = tas.beginning_period_of_availability,
                                       ending_period_of_availabil = tas.ending_period_of_availability,
                                       availability_type_code = None,
                                       main_account_code = tas.main_account_code,
                                       sub_account_code = None)
    # And add a row for a different piid
    second_piid_row_one = AwardFinancialFactory(transaction_obligated_amou = 9999, piid = piid_two,
                                                allocation_transfer_agency = None)
    third_piid_row_one = AwardFinancialFactory(transaction_obligated_amou = 8888, piid = piid_three,
                                               allocation_transfer_agency = 123)
    third_piid_row_two = AwardFinancialFactory(transaction_obligated_amou = 8888, piid = piid_three,
                                               allocation_transfer_agency = None)

    first_ap_row = AwardProcurementFactory(piid = piid, federal_action_obligation = -1100)
    second_ap_row = AwardProcurementFactory(piid = piid, federal_action_obligation = -10)
    third_ap_row = AwardProcurementFactory(piid = piid, federal_action_obligation = -1)
    second_piid_ap_row = AwardProcurementFactory(piid = piid_two, federal_action_obligation = -9999)
    third_piid_ap_row = AwardProcurementFactory(piid = piid_three, federal_action_obligation = -9999)

    errors = number_of_errors(_FILE, database, models=[first_piid_row_one, first_piid_row_two, second_piid_row_one,
       third_piid_row_one, first_ap_row, second_ap_row, third_ap_row, second_piid_ap_row, third_piid_ap_row,
       third_piid_row_two])
    assert errors == 0


def test_failure(database):
    """ Test that a three digit object class with no flag is an error"""
    # Create a 12 character random piid
    piid = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(12))
    piid_two = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(12))
    first_piid_row_one = AwardFinancialFactory(transaction_obligated_amou = 1100, piid = piid, allocation_transfer_agency = None)
    first_piid_row_two = AwardFinancialFactory(transaction_obligated_amou = 11, piid = piid, allocation_transfer_agency = None)
    # And add a row that shouldn't be included
    second_piid_row_one = AwardFinancialFactory(transaction_obligated_amou = 9999, piid = piid_two, allocation_transfer_agency = None)
    first_ap_row = AwardProcurementFactory(piid = piid, federal_action_obligation = -1100)
    second_ap_row = AwardProcurementFactory(piid = piid, federal_action_obligation = -10)
    other_piid_ap_row = AwardProcurementFactory(piid = piid_two, federal_action_obligation = -1111)

    errors = number_of_errors(_FILE, database, models=[first_piid_row_one, first_piid_row_two, second_piid_row_one, first_ap_row, second_ap_row, other_piid_ap_row])
    assert errors == 2