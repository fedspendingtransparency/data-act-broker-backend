from factory import fuzzy
from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b18_object_class_program_activity_2'

skip_three_digit = {
        "submission_id":fuzzy.FuzzyInteger(9999),
        "object_class":fuzzy.FuzzyInteger(999),
        "by_direct_reimbursable_fun":"d"
}

skip_blank_flag = {
        "submission_id":fuzzy.FuzzyInteger(9999),
        "object_class":fuzzy.FuzzyInteger(9999),
        "by_direct_reimbursable_fun":""
}

match_d_value = {
        "submission_id":fuzzy.FuzzyInteger(9999),
        "object_class":fuzzy.FuzzyInteger(1000,1999),
        "by_direct_reimbursable_fun":"d"
}

match_r_value = {
        "submission_id":fuzzy.FuzzyInteger(9999),
        "object_class":fuzzy.FuzzyInteger(2000,2999),
        "by_direct_reimbursable_fun":"r"
}

mismatch_d_value = {
        "submission_id":fuzzy.FuzzyInteger(9999),
        "object_class":fuzzy.FuzzyInteger(2000,2999),
        "by_direct_reimbursable_fun":"d"
}

mismatch_r_value = {
        "submission_id":fuzzy.FuzzyInteger(9999),
        "object_class":fuzzy.FuzzyInteger(1000,1999),
        "by_direct_reimbursable_fun":"r"
}

def test_column_headers(database):
    expected_subset = {"row_number", "object_class", "by_direct_reimbursable_fun"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset <= actual

def test_success(database):
    """ Test that a four digit object class with no flag is a success, and a three digit object class with a flag is a success"""
    ocpa_list = []
    for value_dict in [skip_three_digit, skip_blank_flag, match_d_value, match_r_value]:
        ocpa_list.append(ObjectClassProgramActivityFactory(**value_dict))

    errors = number_of_errors(_FILE, database, models=ocpa_list)
    assert errors == 0


def test_failure(database):
    """ Test that a three digit object class with no flag is an error"""
    ocpa_list = []
    for value_dict in [mismatch_d_value, mismatch_r_value]:
        ocpa_list.append(ObjectClassProgramActivityFactory(**value_dict))

    errors = number_of_errors(_FILE, database, models=ocpa_list)
    assert errors == 2