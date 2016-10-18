from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from dataactcore.models.jobModels import PublishStatus
from dataactcore.models.lookups import PUBLISH_STATUS, PUBLISH_STATUS_DICT
from tests.unit.dataactvalidator.utils import number_of_errors, insert_submission


_FILE = 'a16_object_class_program_activity'


def test_success(database):
    for ps in PUBLISH_STATUS:
        status = PublishStatus(publish_status_id=ps.id, name=ps.name, description=ps.desc)
        database.session.merge(status)
    database.session.commit()

    # gross_outlays_delivered_or_fyb populated does not require a previous submission
    sub_new = SubmissionFactory()
    ocpa_new = ObjectClassProgramActivityFactory(submission_id = sub_new.submission_id)
    assert number_of_errors(_FILE, database, submission = sub_new, models=[ocpa_new]) == 0

    # gross_outlays_delivered_or_fyb can be null if previous published submission shares cgac and fiscal year
    sub_prev_published = SubmissionFactory(publish_status_id = PUBLISH_STATUS_DICT['published'])
    insert_submission(database, sub_prev_published)
    sub_new_published = SubmissionFactory(cgac_code = sub_prev_published.cgac_code, reporting_fiscal_year = sub_prev_published.reporting_fiscal_year)
    ocpa_prev_published = ObjectClassProgramActivityFactory(submission_id = sub_prev_published.submission_id)
    ocpa_new_published = ObjectClassProgramActivityFactory(submission_id = sub_new_published.submission_id, 
       gross_outlays_delivered_or_fyb = None, ussgl480100_undelivered_or_fyb = None)
    assert number_of_errors(_FILE, database, submission = sub_new_published,
                      models=[ocpa_prev_published, ocpa_new_published]) == 0

    # Previous submission marked as publishable also allows null
    sub_prev_publishable = SubmissionFactory(publishable = True)
    insert_submission(database, sub_prev_publishable)
    sub_new_publishable = SubmissionFactory(cgac_code = sub_prev_publishable.cgac_code, reporting_fiscal_year = sub_prev_publishable.reporting_fiscal_year)
    ocpa_prev_publishable = ObjectClassProgramActivityFactory(submission_id = sub_prev_publishable.submission_id)
    ocpa_new_publishable = ObjectClassProgramActivityFactory(submission_id = sub_new_publishable.submission_id, gross_outlays_delivered_or_fyb = None)
    assert number_of_errors(_FILE, database, submission = sub_new_publishable,
                      models=[ocpa_prev_publishable, ocpa_new_publishable]) == 0


def test_failure(database):
    for ps in PUBLISH_STATUS:
        status = PublishStatus(publish_status_id=ps.id, name=ps.name, description=ps.desc)
        database.session.merge(status)
    database.session.commit()

    # No previous submission and null gross_outlays_delivered_or_fyb
    sub_new = SubmissionFactory()
    ocpa_new = ObjectClassProgramActivityFactory(submission_id = sub_new.submission_id, gross_outlays_delivered_or_fyb = None)
    assert number_of_errors(_FILE, database, submission = sub_new, models=[ocpa_new]) == 1

    # previous submission exists but is unpublished and has not been marked publishable
    sub_prev_published = SubmissionFactory(publish_status_id = PUBLISH_STATUS_DICT['unpublished'], publishable = False)
    insert_submission(database, sub_prev_published)
    sub_new_published = SubmissionFactory(cgac_code = sub_prev_published.cgac_code, reporting_fiscal_year = sub_prev_published.reporting_fiscal_year)
    ocpa_prev_published = ObjectClassProgramActivityFactory(submission_id = sub_prev_published.submission_id)
    ocpa_new_published = ObjectClassProgramActivityFactory(submission_id = sub_new_published.submission_id,
       ussgl480100_undelivered_or_fyb = None, ussgl490800_undelivered_or_fyb = None)
    assert number_of_errors(_FILE, database, submission = sub_new_published,
                      models=[ocpa_prev_published, ocpa_new_published]) == 1