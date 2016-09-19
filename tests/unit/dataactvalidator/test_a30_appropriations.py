from dataactcore.models.stagingModels import Appropriation
from dataactcore.models.stagingModels import ObjectClassProgramActivity
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a30_appropriations'
_TAS = 'a30_appropriations_tas'


def test_column_headers(database):
    expected_subset = {'row_number', 'allocation_transfer_agency', 'agency_identifier',
        'beginning_period_of_availa', 'ending_period_of_availabil',
        'availability_type_code', 'main_account_code', 'sub_account_code'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that TAS values in File A should exist in File B for the same reporting period """
    tas = "".join([_TAS, "_success"])

    ap = Appropriation(job_id=1, row_number=1, tas=tas)

    op = ObjectClassProgramActivity(job_id=1, row_number=1, tas=tas)

    assert number_of_errors(_FILE, database, models=[ap, op]) == 0


def test_failure(database):
    """ Tests that TAS values in File A do not exist in File B for the same reporting period """
    tas = "".join([_TAS, "_success"])

    ap = Appropriation(job_id=1, row_number=1, tas=tas)

    op = ObjectClassProgramActivity(job_id=1, row_number=1, tas='1')

    assert number_of_errors(_FILE, database, models=[ap, op]) == 1
