from factory import fuzzy
from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b18_object_class_program_activity_1'

not_req_dict = {
        "submission_id":fuzzy.FuzzyInteger(9999),
        "object_class":fuzzy.FuzzyInteger(1000,9999),
        "by_direct_reimbursable_fun":""
}

req_present_dict = {
        "submission_id":fuzzy.FuzzyInteger(9999),
        "object_class":fuzzy.FuzzyInteger(999),
        "by_direct_reimbursable_fun":"d"
}

req_absent_dict = {
        "submission_id":fuzzy.FuzzyInteger(9999),
        "object_class":fuzzy.FuzzyInteger(999),
        "by_direct_reimbursable_fun":""
}

def test_column_headers(database):
    expected_subset = {"row_number", "object_class", "by_direct_reimbursable_fun"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset <= actual

def test_success(database):
    """ Test that a four digit object class with no flag is a success, and a three digit object class with a flag is a success"""
    not_req_ocpa = ObjectClassProgramActivityFactory(**not_req_dict)
    req_present_ocpa = ObjectClassProgramActivityFactory(**req_present_dict)

    errors = number_of_errors(_FILE, database, models=[not_req_ocpa, req_present_ocpa])
    assert errors == 0


def test_failure(database):
    """ Test that a three digit object class with no flag is an error"""
    req_absent_ocpa = ObjectClassProgramActivityFactory(**req_absent_dict)

    errors = number_of_errors(_FILE, database, models=[req_absent_ocpa])
    assert errors == 1