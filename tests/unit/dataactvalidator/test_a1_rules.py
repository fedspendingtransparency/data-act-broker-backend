"""Test file for the three version of the A1 rule. We abstract away the
differences into parametrized pytests"""
import pytest

from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import (
    AppropriationFactory, AwardFinancialFactory,
    ObjectClassProgramActivityFactory
)
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_factories = {
    'a1_appropriations': AppropriationFactory,
    'a1_award_financial': AwardFinancialFactory,
    'a1_object_class_program_activity': ObjectClassProgramActivityFactory
}


@pytest.mark.parametrize('sql_file', _factories.keys())
def test_column_headers(database, sql_file):
    expected_subset = {
        "row_number", "allocation_transfer_agency", "agency_identifier",
        "beginning_period_of_availa", "ending_period_of_availabil",
        "availability_type_code", "main_account_code", "sub_account_code"
    }

    actual = set(query_columns(sql_file, database))
    assert expected_subset == actual


@pytest.mark.parametrize('sql_file,factory', _factories.items())
def test_success_populated(database, sql_file, factory):
    """TAS values can be found when their components match"""
    tas = TASFactory()
    model = factory(
        allocation_transfer_agency=tas.allocation_transfer_agency,
        agency_identifier=tas.agency_identifier,
        beginning_period_of_availa=tas.beginning_period_of_availability,
        ending_period_of_availabil=tas.ending_period_of_availability,
        availability_type_code=tas.availability_type_code,
        main_account_code=tas.main_account_code,
        sub_account_code=tas.sub_account_code
    )
    # TAS needs to be open during the submission
    submission = SubmissionFactory(
        reporting_start_date=tas.internal_start_date,
        reporting_end_date=tas.internal_start_date
    )

    errors = number_of_errors(
        sql_file, database, submission, models=[tas, model]
    )
    assert errors == 0


@pytest.mark.parametrize('sql_file,factory', _factories.items())
def test_success_null(database, sql_file, factory):
    """TAS values can be found when they are null"""
    tas = TASFactory(
        allocation_transfer_agency=None,
        availability_type_code=None,
        sub_account_code=None
    )
    model = factory(
        allocation_transfer_agency=tas.allocation_transfer_agency,
        agency_identifier=tas.agency_identifier,
        beginning_period_of_availa=tas.beginning_period_of_availability,
        ending_period_of_availabil=tas.ending_period_of_availability,
        availability_type_code=tas.availability_type_code,
        main_account_code=tas.main_account_code,
        sub_account_code=tas.sub_account_code
    )
    # TAS needs to be open during the submission
    submission = SubmissionFactory(
        reporting_start_date=tas.internal_start_date,
        reporting_end_date=tas.internal_start_date
    )

    errors = number_of_errors(
        sql_file, database, submission, models=[tas, model]
    )
    assert errors == 0


@pytest.mark.parametrize('sql_file,factory', _factories.items())
def test_failure_populated(database, sql_file, factory):
    """Non-matching (but populated) TAS's produce errors"""
    tas = TASFactory(agency_identifier=100)
    model = factory(
        allocation_transfer_agency=tas.allocation_transfer_agency,
        agency_identifier=102,
        beginning_period_of_availa=tas.beginning_period_of_availability,
        ending_period_of_availabil=tas.ending_period_of_availability,
        availability_type_code=tas.availability_type_code,
        main_account_code=tas.main_account_code,
        sub_account_code=tas.sub_account_code
    )

    # TAS is open during the submission
    submission = SubmissionFactory(
        reporting_start_date=tas.internal_start_date,
        reporting_end_date=tas.internal_start_date
    )

    # Non-overlapping ranges of agency IDs should generate two errors
    errors = number_of_errors(
        sql_file, database, submission, models=[tas, model]
    )
    assert errors == 1


@pytest.mark.parametrize('sql_file,factory', _factories.items())
def test_failure_null(database, sql_file, factory):
    """Non-matching TAS's (with nulls) produce errors"""
    tas = TASFactory(
        agency_identifier=101,
        allocation_transfer_agency=None,
        availability_type_code=None,
        sub_account_code=None
    )
    model = factory(
        allocation_transfer_agency=tas.allocation_transfer_agency,
        agency_identifier=102,
        beginning_period_of_availa=tas.beginning_period_of_availability,
        ending_period_of_availabil=tas.ending_period_of_availability,
        availability_type_code=tas.availability_type_code,
        main_account_code=tas.main_account_code,
        sub_account_code=tas.sub_account_code
    )

    # TAS is open during the submission
    submission = SubmissionFactory(
        reporting_start_date=tas.internal_start_date,
        reporting_end_date=tas.internal_start_date
    )

    # Non-overlapping ranges of agency IDs should generate two errors
    errors = number_of_errors(
        sql_file, database, submission, models=[tas, model]
    )
    assert errors == 1
