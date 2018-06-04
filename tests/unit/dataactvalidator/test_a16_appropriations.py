from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from dataactcore.models.lookups import PUBLISH_STATUS_DICT
from tests.unit.dataactvalidator.utils import number_of_errors, insert_submission, populate_publish_status


_FILE = 'a16_appropriations'


def test_value_present(database):
    """budget_authority_unobligat_fyb populated does not require a previous submission"""
    populate_publish_status(database)
    sub_new = SubmissionFactory()
    ap_new = AppropriationFactory(submission_id=sub_new.submission_id)
    assert number_of_errors(_FILE, database, submission=sub_new, models=[ap_new]) == 0


def test_previous_published(database):
    """ budget_authority_unobligat_fyb can be null if previous published submission shares cgac and fiscal year """
    populate_publish_status(database)
    sub_prev_published = SubmissionFactory(publish_status_id=PUBLISH_STATUS_DICT['published'])
    insert_submission(database, sub_prev_published)
    sub_new_published = SubmissionFactory(cgac_code=sub_prev_published.cgac_code,
                                          reporting_fiscal_year=sub_prev_published.reporting_fiscal_year)
    ap_new_published = AppropriationFactory(submission_id=sub_new_published.submission_id,
                                            budget_authority_unobligat_fyb=None)
    assert number_of_errors(_FILE, database, submission=sub_new_published,
                            models=[ap_new_published]) == 0


def test_previous_publishable(database):
    """Previous submission marked as publishable also allows null"""
    populate_publish_status(database)
    sub_prev_publishable = SubmissionFactory(publishable=True)
    insert_submission(database, sub_prev_publishable)
    sub_new_publishable = SubmissionFactory(cgac_code=sub_prev_publishable.cgac_code,
                                            reporting_fiscal_year=sub_prev_publishable.reporting_fiscal_year)
    ap_new_publishable = AppropriationFactory(submission_id=sub_new_publishable.submission_id,
                                              budget_authority_unobligat_fyb=None)
    assert number_of_errors(_FILE, database, submission=sub_new_publishable,
                            models=[ap_new_publishable]) == 0


def test_no_previous_submission(database):
    """ No previous submission and null budget_authority_unobligat_fyb"""
    populate_publish_status(database)
    sub_new = SubmissionFactory()
    ap_new = AppropriationFactory(submission_id=sub_new.submission_id, budget_authority_unobligat_fyb=None)
    assert number_of_errors(_FILE, database, submission=sub_new, models=[ap_new]) == 1


def test_previous_unpublished(database):
    """ previous submission exists but is unpublished and has not been marked publishable """
    populate_publish_status(database)
    sub_prev_published = SubmissionFactory(publish_status_id=PUBLISH_STATUS_DICT['unpublished'], publishable=False)
    insert_submission(database, sub_prev_published)
    sub_new_published = SubmissionFactory(cgac_code=sub_prev_published.cgac_code,
                                          reporting_fiscal_year=sub_prev_published.reporting_fiscal_year)
    ap_new_published = AppropriationFactory(submission_id=sub_new_published.submission_id,
                                            budget_authority_unobligat_fyb=None)
    assert number_of_errors(_FILE, database, submission=sub_new_published,
                            models=[ap_new_published]) == 1
