from dataactcore.models.stagingModels import Appropriation
from dataactcore.models.jobModels import Submission
from dataactcore.models.domainModels import SF133
from tests.unit.dataactvalidator.utils import insert_submission, run_sql_rule


_FILE = 'a6_appropriations'
_TAS = 'a6_appropriations_tas'


def test_success(database):
    tas = "".join([_TAS, "_success"])

    submission_id = insert_submission(database.jobDb, Submission(user_id=1, reporting_fiscal_period=1,
                                                                 reporting_fiscal_year=2016))

    sf = SF133(line=1910, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
               main_account_code="000", sub_account_code="000")
    ap = Appropriation(job_id=1, row_number=1, tas=tas, budget_authority_available_cpe=1)

    assert run_sql_rule(_FILE, database.stagingDb, submission_id, sf, ap) == 0


def test_failure(database):
    tas = "".join([_TAS, "_failure"])

    submission_id = insert_submission(database.jobDb, Submission(user_id=1, reporting_fiscal_period=1,
                                                                 reporting_fiscal_year=2016))

    sf = SF133(line=1910, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
               main_account_code="000", sub_account_code="000")
    ap = Appropriation(job_id=1, row_number=1, tas=tas, budget_authority_available_cpe=0)

    assert run_sql_rule(_FILE, database.stagingDb, submission_id, sf, ap) == 1

