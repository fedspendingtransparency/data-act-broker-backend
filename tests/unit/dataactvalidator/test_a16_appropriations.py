from dataactcore.models.stagingModels import Appropriation
from tests.unit.dataactvalidator.utils import run_sql_rule


_FILE = 'a16_appropriations'


def test_success(database):
    award = Appropriation(job_id=1, row_number=1)
    assert run_sql_rule(_FILE, database.stagingDb, award)


def test_null_authority(database):
    award = Appropriation(job_id=1, row_number=1, is_first_quarter=True)
    assert not run_sql_rule(_FILE, database.stagingDb, award)


def test_nonnull_authority(database):
    award = Appropriation(job_id=1, row_number=1, is_first_quarter=True,
                          budget_authority_unobligat_fyb=5)
    assert run_sql_rule(_FILE, database.stagingDb, award)
