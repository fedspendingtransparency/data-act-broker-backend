from random import randint, choice
from string import ascii_uppercase, ascii_lowercase, digits
from tests.unit.dataactcore.factories.staging import AwardFinancialFactory, AwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'c23_award_financial_3'

def test_column_headers(database):
    expected_subset = {"row_number", "transaction_obligated_amou_sum", "federal_action_obligation_sum",
                       "original_loan_subsidy_cost_sum"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset <= actual

def test_success(database):
    """ Test that a four digit object class with no flag is a success, and a three digit object class with a flag is a success"""
    # Create a 12 character random fain
    fain = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(12))
    fain_two = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(12))
    fain_three = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(12))
    first_fain_row_one = AwardFinancialFactory(transaction_obligated_amou = 1100, fain = fain,
                                               allocation_transfer_agency = None)
    first_fain_row_two = AwardFinancialFactory(transaction_obligated_amou = 11, fain = fain,
                                               allocation_transfer_agency = None)
    # And add a row for a different fain
    second_fain_row_one = AwardFinancialFactory(transaction_obligated_amou = 9999, fain = fain_two,
                                                allocation_transfer_agency = None)
    third_fain_row_one = AwardFinancialFactory(transaction_obligated_amou = 8888, fain = fain_three,
                                               allocation_transfer_agency = 123)

    first_afa_row = AwardFinancialAssistanceFactory(fain = fain, federal_action_obligation = -1100,
                                                    original_loan_subsidy_cost = None)
    second_afa_row = AwardFinancialAssistanceFactory(fain = fain, federal_action_obligation = -10,
                                                     original_loan_subsidy_cost = None)
    third_afa_row = AwardFinancialAssistanceFactory(fain = fain, original_loan_subsidy_cost = -1, assistance_type = '08',
                                                    federal_action_obligation = None)
    wrong_type_afa_row = AwardFinancialAssistanceFactory(fain = fain, original_loan_subsidy_cost = -2222,
                                                         assistance_type = '09', federal_action_obligation = None)
    other_fain_afa_row = AwardFinancialAssistanceFactory(fain = fain_two, federal_action_obligation = -9999,
                                                         original_loan_subsidy_cost = None)
    third_fain_ap_row = AwardFinancialAssistanceFactory(fain = fain_three, federal_action_obligation = -9999)                                                         

    errors = number_of_errors(_FILE, database, models=[first_fain_row_one, first_fain_row_two, second_fain_row_one,
       first_afa_row, second_afa_row, third_afa_row, wrong_type_afa_row, other_fain_afa_row, third_fain_row_one,
       third_fain_ap_row])
    assert errors == 0


def test_failure(database):
    """ Test that a three digit object class with no flag is an error"""
    # Create a 12 character random fain
    fain = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(12))
    fain_two = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(12))
    first_fain_row_one = AwardFinancialFactory(transaction_obligated_amou = 1100, fain = fain,
                                               allocation_transfer_agency = None)
    first_fain_row_two = AwardFinancialFactory(transaction_obligated_amou = 11, fain = fain,
                                               allocation_transfer_agency = None)
    # And add a row that shouldn't be included
    second_fain_row_one = AwardFinancialFactory(transaction_obligated_amou = 9999, fain = fain_two,
                                                allocation_transfer_agency = None)
    first_afa_row = AwardFinancialAssistanceFactory(fain = fain, federal_action_obligation = -1100,
                                                    original_loan_subsidy_cost = None)
    second_afa_row = AwardFinancialAssistanceFactory(fain = fain, federal_action_obligation = -10,
                                                     original_loan_subsidy_cost = None)
    other_fain_afa_row = AwardFinancialAssistanceFactory(fain = fain_two, federal_action_obligation = -9999,
                                                         original_loan_subsidy_cost = None)
    other_fain_loan_afa_row = AwardFinancialAssistanceFactory(fain = fain_two, federal_action_obligation = None,
                                                         original_loan_subsidy_cost = -1000, assistance_type = '07')

    errors = number_of_errors(_FILE, database, models=[first_fain_row_one, first_fain_row_two, second_fain_row_one, first_afa_row, second_afa_row, other_fain_afa_row, other_fain_loan_afa_row])
    assert errors == 2