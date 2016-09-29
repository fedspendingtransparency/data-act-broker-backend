from random import randint, choice
from string import ascii_uppercase, ascii_lowercase, digits
from tests.unit.dataactcore.factories.staging import AwardFinancialFactory, AwardProcurementFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'c23_award_financial_1'

def test_column_headers(database):
    expected_subset = {"row_number", "transaction_obligated_amou_sum", "federal_action_obligation_sum"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset <= actual

def test_success(database):
    """ Test that a four digit object class with no flag is a success, and a three digit object class with a flag is a success"""
    # Create a 12 character random piid
    piid = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(12))
    piid_two = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(12))
    piid_three = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(12))
    first_piid_row_one = AwardFinancialFactory(transaction_obligated_amou = 1100, piid = piid,
                                               allocation_transfer_agency = None)
    first_piid_row_two = AwardFinancialFactory(transaction_obligated_amou = 11, piid = piid,
                                               allocation_transfer_agency = None)
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