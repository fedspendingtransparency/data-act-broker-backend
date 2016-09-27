from dataactcore.models.stagingModels import Appropriation
from dataactcore.models.stagingModels import ObjectClassProgramActivity
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a30_object_class_program_activity'
_TAS = 'a30_object_class_program_activity_tas'


def test_column_headers(database):
    expected_subset = {'row_number', 'allocation_transfer_agency', 'agency_identifier',
        'beginning_period_of_availa', 'ending_period_of_availabil',
        'availability_type_code', 'main_account_code', 'sub_account_code'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that TAS values in File B should exist in File A for the same reporting period """
    tas = "".join([_TAS, "_success"])

    op = ObjectClassProgramActivity(job_id=1, row_number=1, tas=tas)

    ap = Appropriation(job_id=1, row_number=1, tas=tas)

    assert number_of_errors(_FILE, database, models=[op, ap]) == 0


def test_failure(database):
    """ Tests that TAS values in File B do not exist in File A for the same reporting period """
    tas = "".join([_TAS, "_success"])

    op = ObjectClassProgramActivity(job_id=1, row_number=1, tas=tas)

    ap = Appropriation(job_id=1, row_number=1, tas='1')

    assert number_of_errors(_FILE, database, models=[op, ap]) == 1
