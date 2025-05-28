from datetime import date

import pytest

from dataactcore.config import CONFIG_BROKER
from dataactcore.utils.ResponseError import ResponseError
from dataactvalidator.validation_handlers import validationManager

from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import (
    AppropriationFactory,
    AwardFinancialFactory,
    ObjectClassProgramActivityFactory,
)


with_factory_parameters = pytest.mark.parametrize(
    "factory", (AppropriationFactory, AwardFinancialFactory, ObjectClassProgramActivityFactory)
)


@with_factory_parameters
def test_update_account_nums_has_match_open_ended(database, factory):
    """If there are models which match the TAS (with an undefined end date), they should be modified"""
    sess = database.session
    submission = SubmissionFactory(reporting_start_date=date(2010, 10, 1), reporting_end_date=date(2010, 10, 1))
    sess.add(submission)
    sess.flush()
    tas = TASFactory(internal_start_date=date(2010, 9, 1), tas="ABC")
    model = factory(submission_id=submission.submission_id, tas=tas.tas, account_num=None)
    sess.add_all([tas, model])
    sess.commit()

    validationManager.update_account_nums(model.__class__, submission.submission_id)

    model = sess.query(model.__class__).one()  # we'll only have one entry
    assert model.account_num == tas.account_num


@with_factory_parameters
def test_update_account_nums_has_match_closed(database, factory):
    """If there are models which match the TAS (with an defined end date),
    they should be modified"""
    sess = database.session
    submission = SubmissionFactory(reporting_start_date=date(2010, 10, 10), reporting_end_date=date(2010, 10, 31))
    sess.add(submission)
    sess.flush()
    tas = TASFactory(internal_start_date=date(2010, 9, 1), internal_end_date=date(2010, 10, 15), tas="ABC")
    model = factory(submission_id=submission.submission_id, tas=tas.tas, account_num=None)
    sess.add_all([tas, model])
    sess.commit()

    validationManager.update_account_nums(model.__class__, submission.submission_id)

    model = sess.query(model.__class__).one()  # we'll only have one entry
    assert model.account_num == tas.account_num


@with_factory_parameters
def test_update_account_nums_no_match(database, factory):
    """If a TAS doesn't share fields, we don't expect a match"""
    sess = database.session
    submission = SubmissionFactory(reporting_start_date=date(2010, 10, 10), reporting_end_date=date(2010, 10, 31))
    sess.add(submission)
    sess.flush()
    tas = TASFactory(internal_start_date=date(2010, 9, 1))
    # note these will have different fields
    model = factory(submission_id=submission.submission_id)
    assert model.account_num is None
    sess.add_all([tas, model])
    sess.commit()

    validationManager.update_account_nums(model.__class__, submission.submission_id)

    model = sess.query(model.__class__).one()  # we'll only have one entry
    assert model.account_num is None


@with_factory_parameters
def test_update_account_nums_bad_dates(database, factory):
    """If the relevant TAS does not overlap the date of the submission, it
    should not be used"""
    sess = database.session
    submission = SubmissionFactory(reporting_start_date=date(2010, 10, 1), reporting_end_date=date(2010, 10, 1))
    sess.add(submission)
    sess.flush()
    tas = TASFactory(internal_start_date=date(2011, 1, 1), tas="ABC")
    model = factory(submission_id=submission.submission_id, tas=tas.tas, account_num=None)
    sess.add_all([tas, model])
    sess.commit()

    validationManager.update_account_nums(model.__class__, submission.submission_id)

    model = sess.query(model.__class__).one()  # we'll only have one entry
    assert model.account_num is None


@pytest.mark.usefixtures("database")
def test_attempt_validate_deleted_job():
    error = None
    validation_manager = validationManager.ValidationManager(is_local=CONFIG_BROKER["local"])
    try:
        validation_manager.validate_job(12345678901234567890)
    except ResponseError as e:
        error = e

    assert error is not None
    assert str(error) == "Job ID 12345678901234567890 not found in database"
