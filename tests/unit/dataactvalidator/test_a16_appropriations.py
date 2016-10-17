from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactvalidator.utils import error_rows, number_of_errors, insert_submission


_FILE = 'a16_appropriations'


def test_success(database):
    sub_new = SubmissionFactory()
    sub_prev_published = SubmissionFactory()
    sub_new_published = SubmissionFactory(cgac_code = sub_prev_published.cgac_code, reporting_fiscal_year = sub_prev_published.reporting_fiscal_year)
    sub_prev_publishable = SubmissionFactory()
    sub_new_publishable = SubmissionFactory(cgac_code = sub_prev_publishable.cgac_code, reporting_fiscal_year = sub_prev_publishable.reporting_fiscal_year)
    insert_submission(database, sub_new)
    insert_submission(database, sub_prev_published)
    insert_submission(database, sub_new_published)
    insert_submission(database, sub_prev_publishable)
    insert_submission(database, sub_new_publishable)
    # budget_authority_unobligat_fyb can be null if previous submission shares cgac and fiscal year
    ap_new = AppropriationFactory(submission_id = sub_new.submission_id)
    ap_prev_published = AppropriationFactory(submission_id = sub_prev_published.submission_id)
    ap_new_published = AppropriationFactory(submission_id = sub_new_published.submission_id, budget_authority_unobligat_fyb = None)
    ap_prev_publishable = AppropriationFactory(submission_id = sub_prev_publishable.submission_id)
    ap_new_publishable = AppropriationFactory(submission_id = sub_new_publishable.submission_id, budget_authority_unobligat_fyb = None)
    assert error_rows(_FILE, database, models=[sub_new, sub_prev_published, sub_new_published, sub_prev_publishable,
                                               sub_new_publishable, ap_new, ap_prev_published, ap_new_published,
                                               ap_prev_publishable, ap_new_publishable]) == []


def test_null_authority(database):
    award = Appropriation(job_id=1, row_number=1)
    assert number_of_errors(_FILE, database, models=[award]) == 1


def test_nonnull_authority(database):
    award = Appropriation(job_id=1, row_number=1, budget_authority_unobligat_fyb=5)
    assert error_rows(_FILE, database, models=[award]) == []
