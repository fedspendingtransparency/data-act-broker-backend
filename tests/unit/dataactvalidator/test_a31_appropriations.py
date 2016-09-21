from dataactcore.models.stagingModels import Appropriation
from tests.unit.dataactvalidator.utils import number_of_errors


_FILE = 'a31_appropriations'
_TAS = 'a31_appropriations_tas'


def test_success(database):
    """ Tests that SF 133 amount sum for line 2500 matches Appropriation status_of_budgetary_resour_cpe
        for the specified fiscal year and period """
    tas = "".join([_TAS, "_success"])

    ap = Appropriation(job_id=1, row_number=1, tas=tas, status_of_budgetary_resour_cpe=1)

    assert number_of_errors(_FILE, database, models=[sf, ap]) == 0


def test_failure(database):
    """ Tests that SF 133 amount sum for line 2500 does not match Appropriation status_of_budgetary_resour_cpe
        for the specified fiscal year and period """
    tas = "".join([_TAS, "_failure"])

    ap = Appropriation(job_id=1, row_number=1, tas=tas, status_of_budgetary_resour_cpe=0)

    assert number_of_errors(_FILE, database, models=[sf, ap]) == 1
