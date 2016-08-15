from dataactcore.models.stagingModels import ObjectClassProgramActivity
from dataactcore.models.jobModels import Submission
from dataactcore.models.domainModels import SF133
from tests.unit.dataactvalidator.utils import insert_submission, run_sql_rule


_FILE = 'b15_object_class_program_activity'
_TAS = 'b15_object_class_program_activity_tas'


def test_success(database):
    tas = "".join([_TAS, "_success"])

    submission_id = insert_submission(database.jobDb, Submission(user_id=1, reporting_fiscal_period=1,
                                                                 reporting_fiscal_year=2016))

    sf = SF133(line=2104, tas=tas, period=1, fiscal_year=2016, amount=5, agency_identifier="sys",
               main_account_code="000", sub_account_code="000")

    op = ObjectClassProgramActivity(job_id=1, row_number=1, tas=tas, by_direct_reimbursable_fun='r',
                                    ussgl480100_undelivered_or_cpe=1, ussgl480100_undelivered_or_fyb=1,
                                    ussgl480200_undelivered_or_cpe=1, ussgl480200_undelivered_or_fyb=1,
                                    ussgl488100_upward_adjustm_cpe=1, ussgl488200_upward_adjustm_cpe=1,
                                    ussgl490100_delivered_orde_cpe=1, ussgl490100_delivered_orde_fyb=1,
                                    ussgl490200_delivered_orde_cpe=1, ussgl490800_authority_outl_cpe=1,
                                    ussgl490800_authority_outl_fyb=1, ussgl498100_upward_adjustm_cpe=1,
                                    ussgl498200_upward_adjustm_cpe=1)

    assert run_sql_rule(_FILE, database.stagingDb, submission_id, sf, op) == 0


def test_failure(database):
    tas = "".join([_TAS, "_failure"])

    submission_id = insert_submission(database.jobDb, Submission(user_id=1, reporting_fiscal_period=1,
                                                                 reporting_fiscal_year=2016))

    sf = SF133(line=2104, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
               main_account_code="000", sub_account_code="000")

    op = ObjectClassProgramActivity(job_id=1, row_number=1, tas=tas, by_direct_reimbursable_fun='r',
                                    ussgl480100_undelivered_or_cpe=1, ussgl480100_undelivered_or_fyb=1,
                                    ussgl480200_undelivered_or_cpe=1, ussgl480200_undelivered_or_fyb=1,
                                    ussgl488100_upward_adjustm_cpe=1, ussgl488200_upward_adjustm_cpe=1,
                                    ussgl490100_delivered_orde_cpe=1, ussgl490100_delivered_orde_fyb=1,
                                    ussgl490200_delivered_orde_cpe=1, ussgl490800_authority_outl_cpe=1,
                                    ussgl490800_authority_outl_fyb=1, ussgl498100_upward_adjustm_cpe=1,
                                    ussgl498200_upward_adjustm_cpe=1)

    assert run_sql_rule(_FILE, database.stagingDb, submission_id, sf, op) == 1

