
from tests.unit.dataactvalidator.utils import number_of_errors


_FILE = 'a31_object_class_program_activity'
_TAS = 'a31_object_class_program_activity_tas'


def test_success(database):
    """ Tests that SF 133 amount sum for line 2500 matches Appropriation status_of_budgetary_resour_cpe
        for the specified fiscal year and period """
    tas = "".join([_TAS, "_success"])

    assert number_of_errors(_FILE, database, models=[sf, ap]) == 0


def test_failure(database):
    """ Tests that SF 133 amount sum for line 2500 does not match Appropriation status_of_budgetary_resour_cpe
        for the specified fiscal year and period """
    tas = "".join([_TAS, "_failure"])

    assert number_of_errors(_FILE, database, models=[sf, ap]) == 1
