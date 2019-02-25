from datetime import date
from unittest.mock import Mock

import pytest

from dataactcore.config import CONFIG_BROKER
from dataactcore.utils.responseException import ResponseException
from dataactvalidator.validation_handlers import validationManager
from dataactvalidator.validation_handlers.errorInterface import ErrorInterface

from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactcore.factories.job import JobFactory, SubmissionFactory
from tests.unit.dataactcore.factories.staging import (AppropriationFactory, AwardFinancialFactory,
                                                      ObjectClassProgramActivityFactory)


with_factory_parameters = pytest.mark.parametrize('factory', (
    AppropriationFactory, AwardFinancialFactory, ObjectClassProgramActivityFactory
))


@with_factory_parameters
def test_update_tas_ids_has_match_open_ended(database, factory):
    """If there are models which match the TAS (with an undefined end date),
    they should be modified"""
    sess = database.session
    submission = SubmissionFactory(reporting_start_date=date(2010, 10, 1), reporting_end_date=date(2010, 10, 1))
    sess.add(submission)
    sess.flush()
    tas = TASFactory(internal_start_date=date(2010, 9, 1))
    model = factory(submission_id=submission.submission_id, **tas.component_dict())
    assert model.tas_id is None
    sess.add_all([tas, model])
    sess.commit()

    validationManager.update_tas_ids(model.__class__, submission.submission_id)

    model = sess.query(model.__class__).one()   # we'll only have one entry
    assert model.tas_id == tas.account_num


@with_factory_parameters
def test_update_tas_ids_has_match_closed(database, factory):
    """If there are models which match the TAS (with an defined end date),
    they should be modified"""
    sess = database.session
    submission = SubmissionFactory(reporting_start_date=date(2010, 10, 10), reporting_end_date=date(2010, 10, 31))
    sess.add(submission)
    sess.flush()
    tas = TASFactory(internal_start_date=date(2010, 9, 1), internal_end_date=date(2010, 10, 15))
    model = factory(submission_id=submission.submission_id, **tas.component_dict())
    assert model.tas_id is None
    sess.add_all([tas, model])
    sess.commit()

    validationManager.update_tas_ids(model.__class__, submission.submission_id)

    model = sess.query(model.__class__).one()   # we'll only have one entry
    assert model.tas_id == tas.account_num


@with_factory_parameters
def test_update_tas_ids_no_match(database, factory):
    """If a TAS doesn't share fields, we don't expect a match"""
    sess = database.session
    submission = SubmissionFactory(reporting_start_date=date(2010, 10, 10), reporting_end_date=date(2010, 10, 31))
    sess.add(submission)
    sess.flush()
    tas = TASFactory(internal_start_date=date(2010, 9, 1))
    # note these will have different fields
    model = factory(submission_id=submission.submission_id)
    assert model.tas_id is None
    sess.add_all([tas, model])
    sess.commit()

    validationManager.update_tas_ids(model.__class__, submission.submission_id)

    model = sess.query(model.__class__).one()   # we'll only have one entry
    assert model.tas_id is None


@with_factory_parameters
def test_update_tas_ids_bad_dates(database, factory):
    """If the relevant TAS does not overlap the date of the submission, it
    should not be used"""
    sess = database.session
    submission = SubmissionFactory(reporting_start_date=date(2010, 10, 1), reporting_end_date=date(2010, 10, 1))
    sess.add(submission)
    sess.flush()
    tas = TASFactory(internal_start_date=date(2011, 1, 1))
    model = factory(submission_id=submission.submission_id, **tas.component_dict())
    assert model.tas_id is None
    sess.add_all([tas, model])
    sess.commit()

    validationManager.update_tas_ids(model.__class__, submission.submission_id)

    model = sess.query(model.__class__).one()   # we'll only have one entry
    assert model.tas_id is None


@pytest.mark.usefixtures('database')
def test_insert_staging_model_failure():
    writer = Mock()
    error_list = ErrorInterface()
    model = AppropriationFactory(row_number=1234, adjustments_to_unobligated_cpe='shoulda-been-a-number')
    job = JobFactory()
    assert not validationManager.insert_staging_model(model, job, writer, error_list)
    assert writer.writerow.call_args[0] == (
        ['Formatting Error', 'Could not write this record into the staging table.', 1234, ''],
    )
    assert len(error_list.rowErrors) == 1
    error = list(error_list.rowErrors.values())[0]
    assert error['firstRow'] == 1234
    assert error['fieldName'] == 'Formatting Error'
    assert error['filename'] == job.filename


@pytest.mark.usefixtures('database')
def test_attempt_validate_deleted_job():
    error = None
    validation_manager = validationManager.ValidationManager(is_local=CONFIG_BROKER['local'])
    try:
        validation_manager.validate_job(12345678901234567890)
    except ResponseException as e:
        error = e

    assert error is not None
    assert str(error) == 'Job ID 12345678901234567890 not found in database'
