from random import randint, choice
from string import ascii_uppercase, ascii_lowercase, digits
from tests.unit.dataactcore.factories.staging import AwardFinancialFactory, AwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'c23_award_financial_1'

def test_column_headers(database):
    expected_subset = {"row_number", "transaction_obligated_amou_sum", "federal_action_obligation_sum"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset <= actual

def test_success(database):
    """ Test that a four digit object class with no flag is a success, and a three digit object class with a flag is a success"""
    # Create a 12 character random uri
    uri = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(12))
    uri_two = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(12))
    first_uri_row_one = AwardFinancialFactory(object_class = randint(1100,1999),
        by_direct_reimbursable_fun = "d", transaction_obligated_amou = 1234, uri = uri)
    first_uri_row_two = AwardFinancialFactory(object_class = randint(1000,1099),
        by_direct_reimbursable_fun = "d", transaction_obligated_amou = 2345, uri = uri)
    # And add a row for a different uri
    second_uri_row_one = AwardFinancialFactory(object_class = randint(1100,1999),
        by_direct_reimbursable_fun = "d", transaction_obligated_amou = 9999, uri = uri_two)
    first_afa_row = AwardFinancialAssistanceFactory(uri = uri, federal_action_obligation = -2468)
    second_afa_row = AwardFinancialAssistanceFactory(uri = uri, federal_action_obligation = -1100)
    third_afa_row = AwardFinancialAssistanceFactory(uri = uri, federal_action_obligation = -11)
    other_uri_afa_row = AwardFinancialAssistanceFactory(uri = uri_two, federal_action_obligation = -9999)

    errors = number_of_errors(_FILE, database, models=[first_uri_row_one, first_uri_row_two, second_uri_row_one, first_afa_row, second_afa_row, third_afa_row, other_uri_afa_row])
    assert errors == 0


def test_failure(database):
    """ Test that a three digit object class with no flag is an error"""
    # Create a 12 character random uri
    uri = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(12))
    uri_two = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(12))
    first_uri_row_one = AwardFinancialFactory(object_class = randint(1100,1999),
        by_direct_reimbursable_fun = "d", transaction_obligated_amou = 1234, uri = uri)
    first_uri_row_two = AwardFinancialFactory(object_class = randint(1000,1099),
        by_direct_reimbursable_fun = "d", transaction_obligated_amou = 2345, uri = uri)
    # And add a row that shouldn't be included
    second_uri_row_one = AwardFinancialFactory(object_class = randint(1100,1999),
        by_direct_reimbursable_fun = "d", transaction_obligated_amou = 9999, uri = uri_two)
    first_afa_row = AwardFinancialAssistanceFactory(uri = uri, federal_action_obligation = -2468)
    second_afa_row = AwardFinancialAssistanceFactory(uri = uri, federal_action_obligation = -1000)
    other_uri_afa_row = AwardFinancialAssistanceFactory(uri = uri_two, federal_action_obligation = -1111)

    errors = number_of_errors(_FILE, database, models=[first_uri_row_one, first_uri_row_two, second_uri_row_one, first_afa_row, second_afa_row, other_uri_afa_row])
    assert errors == 2