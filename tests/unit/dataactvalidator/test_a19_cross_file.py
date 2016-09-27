from random import randint

from dataactcore.models.stagingModels import (
    Appropriation, ObjectClassProgramActivity)
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a19_cross_file'


def test_column_headers(database):
    expected_subset = set([
        'row_number', 'allocation_transfer_agency', 'agency_identifier',
        'beginning_period_of_availa', 'ending_period_of_availabil',
        'availability_type_code', 'main_account_code', 'sub_account_code',
        'obligations_incurred_total_cpe', 'obligations_incurred_by_pr_cpe_sum'
    ])
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


# @todo remove when we're using a factory builder
def set_shared_values(*models):
    job_id, row_number = randint(1, 9999), randint(1, 9999)
    for model in models:
        model.job_id = job_id
        model.row_number = row_number
        model.tas = 'TAS'


def test_sum_matches(database):
    op1_val, op2_val = randint(1, 9999), randint(1, 9999)
    approp_val = -op1_val - op2_val
    op1 = ObjectClassProgramActivity(obligations_incurred_by_pr_cpe=op1_val)
    op2 = ObjectClassProgramActivity(obligations_incurred_by_pr_cpe=op2_val)
    approp = Appropriation(obligations_incurred_total_cpe=approp_val)
    set_shared_values(op1, op2, approp)
    assert number_of_errors(_FILE, database,
                            models=[approp, op1, op2]) == 0


def test_sum_does_not_match(database):
    op1_val, op2_val = randint(1, 9999), randint(1, 9999)
    approp_val = -op1_val - op2_val + randint(1, 9999)
    op1 = ObjectClassProgramActivity(obligations_incurred_by_pr_cpe=op1_val)
    op2 = ObjectClassProgramActivity(obligations_incurred_by_pr_cpe=op2_val)
    approp = Appropriation(obligations_incurred_total_cpe=approp_val)
    set_shared_values(op1, op2, approp)
    assert number_of_errors(_FILE, database,
                            models=[approp, op1, op2]) == 1
