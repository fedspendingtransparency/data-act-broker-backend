from dataactcore.models.stagingModels import Appropriation
from tests.unit.dataactvalidator.utils import error_rows, number_of_errors


_FILE = 'a16_appropriations'


def test_success(database):
    award = Appropriation(job_id=1, row_number=1)
    assert error_rows(_FILE, database, models=[award]) == []


def test_null_authority(database):
    award = Appropriation(job_id=1, row_number=1, is_first_quarter=True)
    assert number_of_errors(_FILE, database, models=[award]) == 1


def test_nonnull_authority(database):
    award = Appropriation(job_id=1, row_number=1, is_first_quarter=True,
                          budget_authority_unobligat_fyb=5)
    assert error_rows(_FILE, database, models=[award]) == []
