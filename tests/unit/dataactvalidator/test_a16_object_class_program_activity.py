from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from dataactcore.models.jobModels import PublishStatus
from dataactcore.models.lookups import PUBLISH_STATUS, PUBLISH_STATUS_DICT
from tests.unit.dataactvalidator.utils import number_of_errors, insert_submission, query_columns


_FILE = 'a16_object_class_program_activity'


def test_column_headers(database):
    expected_subset = {'row_number', 'gross_outlay_amount_by_pro_fyb', 'gross_outlays_delivered_or_fyb',
                       'gross_outlays_undelivered_fyb', 'obligations_delivered_orde_fyb',
                       'obligations_undelivered_or_fyb', 'ussgl480100_undelivered_or_fyb',
                       'ussgl480200_undelivered_or_fyb', 'ussgl490100_delivered_orde_fyb',
                       'ussgl490800_authority_outl_fyb', 'uniqueid_TAS'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def populate_publish_status(database):
    for ps in PUBLISH_STATUS:
        status = PublishStatus(publish_status_id=ps.id, name=ps.name, description=ps.desc)
        database.session.merge(status)
    database.session.commit()


def test_value_present(database):
    """ gross_outlays_delivered_or_fyb populated does not require a previous submission """
    populate_publish_status(database)
    sub_new = SubmissionFactory()
    ocpa_new = ObjectClassProgramActivityFactory(submission_id=sub_new.submission_id)
    assert number_of_errors(_FILE, database, submission=sub_new, models=[ocpa_new]) == 0


def test_previous_published(database):
    """ gross_outlays_delivered_or_fyb can be null if previous published submission shares cgac and fiscal year """
    populate_publish_status(database)
    sub_prev_published = SubmissionFactory(publish_status_id=PUBLISH_STATUS_DICT['published'])
    insert_submission(database, sub_prev_published)
    sub_new_published = SubmissionFactory(cgac_code=sub_prev_published.cgac_code,
                                          reporting_fiscal_year=sub_prev_published.reporting_fiscal_year)
    ocpa_new_published = ObjectClassProgramActivityFactory(submission_id=sub_new_published.submission_id,
                                                           gross_outlays_delivered_or_fyb=None,
                                                           ussgl480100_undelivered_or_fyb=None)
    assert number_of_errors(_FILE, database, submission=sub_new_published,
                            models=[ocpa_new_published]) == 0


def test_previous_publishable(database):
    """ Previous submission marked as publishable also allows null """
    populate_publish_status(database)
    sub_prev_publishable = SubmissionFactory(publishable=True)
    insert_submission(database, sub_prev_publishable)
    sub_new_publishable = SubmissionFactory(cgac_code=sub_prev_publishable.cgac_code,
                                            reporting_fiscal_year=sub_prev_publishable.reporting_fiscal_year)
    ocpa_new_publishable = ObjectClassProgramActivityFactory(submission_id=sub_new_publishable.submission_id,
                                                             gross_outlays_delivered_or_fyb=None)
    assert number_of_errors(_FILE, database, submission=sub_new_publishable,
                            models=[ocpa_new_publishable]) == 0


def test_no_previous_submission(database):
    """ No previous submission and null gross_outlays_delivered_or_fyb """
    populate_publish_status(database)
    sub_new = SubmissionFactory()
    ocpa_new = ObjectClassProgramActivityFactory(submission_id=sub_new.submission_id,
                                                 gross_outlays_delivered_or_fyb=None)
    assert number_of_errors(_FILE, database, submission=sub_new, models=[ocpa_new]) == 1


def test_previous_unpublished(database):
    """ previous submission exists but is unpublished and has not been marked publishable """
    populate_publish_status(database)
    sub_prev_published = SubmissionFactory(publish_status_id=PUBLISH_STATUS_DICT['unpublished'], publishable=False)
    insert_submission(database, sub_prev_published)
    sub_new_published = SubmissionFactory(cgac_code=sub_prev_published.cgac_code,
                                          reporting_fiscal_year=sub_prev_published.reporting_fiscal_year)
    ocpa_new_published = ObjectClassProgramActivityFactory(submission_id=sub_new_published.submission_id,
                                                           ussgl480100_undelivered_or_fyb=None,
                                                           ussgl490800_undelivered_or_fyb=None)
    assert number_of_errors(_FILE, database, submission=sub_new_published,
                            models=[ocpa_new_published]) == 1
