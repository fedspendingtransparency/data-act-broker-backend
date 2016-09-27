from dataactcore.models.stagingModels import AwardFinancial
from dataactcore.models.stagingModels import ObjectClassProgramActivity
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b20_object_class_program_activity'
_TAS = 'b20_object_class_program_activity_tas'


def test_column_headers(database):
    expected_subset = {'row_number', 'allocation_transfer_agency', 'agency_identifier',
                       'beginning_period_of_availa', 'ending_period_of_availabil', 'availability_type_code',
                       'main_account_code', 'sub_account_code', 'program_activity_code', 'object_class'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that all combinations of TAS, program activity code, and object class in File C exist in File B """
    tas = "".join([_TAS, "_success"])

    op = ObjectClassProgramActivity(job_id=1, row_number=1, tas=tas, program_activity_code='1', object_class='1')

    af = AwardFinancial(job_id=1, row_number=1, tas=tas, program_activity_code='1', object_class='1')

    assert number_of_errors(_FILE, database, models=[op, af]) == 0


def test_failure(database):
    """ Tests that all combinations of TAS, program activity code, and object class in File C do not exist in File B """
    tas = "".join([_TAS, "_failure"])

    op = ObjectClassProgramActivity(job_id=1, row_number=1, tas=tas, program_activity_code='1', object_class='1')

    af1 = AwardFinancial(job_id=1, row_number=1, tas=tas, program_activity_code='1', object_class='1')
    af2 = AwardFinancial(job_id=1, row_number=1, tas='1', program_activity_code='1', object_class='1')
    af3 = AwardFinancial(job_id=1, row_number=1, tas=tas, program_activity_code='2', object_class='1')
    af4 = AwardFinancial(job_id=1, row_number=1, tas=tas, program_activity_code='1', object_class='2')

    assert number_of_errors(_FILE, database, models=[op, af1, af2, af3, af4]) == 3
