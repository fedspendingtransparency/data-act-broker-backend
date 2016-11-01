from datetime import date

import pytest

from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'a1_appropriations'

def test_column_headers(database):
    expected_subset = {"row_number", "allocation_transfer_agency", "agency_identifier", "beginning_period_of_availa",
        "ending_period_of_availabil", "availability_type_code", "main_account_code", "sub_account_code"}

    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual

def test_success(database):
    """ Test that TAS values can be found, and null matches work correctly"""
    tas = TASFactory()
    tas_null = TASFactory(
        allocation_transfer_agency=None,
        availability_type_code=None,
        internal_end_date=tas.internal_end_date,
        internal_start_date=tas.internal_start_date,
        sub_account_code=None
    )
    approp = AppropriationFactory(allocation_transfer_agency = tas.allocation_transfer_agency,
                                  agency_identifier = tas.agency_identifier,
                                  beginning_period_of_availa = tas.beginning_period_of_availability,
                                  ending_period_of_availabil = tas.ending_period_of_availability,
                                  availability_type_code = tas.availability_type_code,
                                  main_account_code = tas.main_account_code,
                                  sub_account_code = tas.sub_account_code)
    approp_null = AppropriationFactory(allocation_transfer_agency = tas_null.allocation_transfer_agency,
                                       agency_identifier = tas_null.agency_identifier,
                                       beginning_period_of_availa = tas_null.beginning_period_of_availability,
                                       ending_period_of_availabil = tas_null.ending_period_of_availability,
                                       availability_type_code = tas_null.availability_type_code,
                                       main_account_code = tas_null.main_account_code,
                                       sub_account_code = tas_null.sub_account_code)

    # TAS are open during the submission
    submission = SubmissionFactory(
        reporting_start_date=tas.internal_start_date,
        reporting_end_date=tas.internal_start_date
    )

    errors = number_of_errors(
        _FILE, database, submission,
        models=[tas, tas_null, approp, approp_null]
    )
    assert errors == 0

def test_failure(database):
    """ Test that tas that does not match is an error"""

    tas = TASFactory(agency_identifier=100)
    tas_null = TASFactory(
        agency_identifier=101,
        allocation_transfer_agency=None,
        availability_type_code=None,
        internal_end_date=tas.internal_end_date,
        internal_start_date=tas.internal_start_date,
        sub_account_code=None
    )
    approp = AppropriationFactory(allocation_transfer_agency = tas.allocation_transfer_agency,
                                  agency_identifier = 102,
                                  beginning_period_of_availa = tas.beginning_period_of_availability,
                                  ending_period_of_availabil = tas.ending_period_of_availability,
                                  availability_type_code = tas.availability_type_code,
                                  main_account_code = tas.main_account_code,
                                  sub_account_code = tas.sub_account_code)
    approp_null = AppropriationFactory(allocation_transfer_agency = tas_null.allocation_transfer_agency,
                                       agency_identifier = 103,
                                       beginning_period_of_availa = tas_null.beginning_period_of_availability,
                                       ending_period_of_availabil = tas_null.ending_period_of_availability,
                                       availability_type_code = tas_null.availability_type_code,
                                       main_account_code = tas_null.main_account_code,
                                       sub_account_code = tas_null.sub_account_code)

    # TAS are open during the submission
    submission = SubmissionFactory(
        reporting_start_date=tas.internal_start_date,
        reporting_end_date=tas.internal_start_date
    )

    # Non-overlapping ranges of agency IDs should generate two errors
    errors = number_of_errors(
        _FILE, database, submission, 
        models=[tas, tas_null, approp, approp_null]
    )
    assert errors == 2


def test_tas_expired(database):
    """If a TAS has expired, we shouldn't find it"""
    tas = TASFactory(internal_start_date=date(2015, 1, 1),
                     internal_end_date=date(2016, 1, 1))
    approp = AppropriationFactory(
        allocation_transfer_agency=tas.allocation_transfer_agency,
        agency_identifier=tas.agency_identifier,
        beginning_period_of_availa=tas.beginning_period_of_availability,
        ending_period_of_availabil=tas.ending_period_of_availability,
        availability_type_code=tas.availability_type_code,
        main_account_code=tas.main_account_code,
        sub_account_code=tas.sub_account_code
    )
    submission = SubmissionFactory(
        reporting_start_date=date(2016, 2, 2),
        reporting_end_date=date(2016, 3, 3)
    )

    assert 1 == number_of_errors(
        _FILE, database, submission, models=[tas, approp])


def test_tas_in_future(database):
    """If a TAS hasn't began yet, we shouldn't find it"""
    tas = TASFactory(internal_start_date=date(2015, 1, 1),
                     internal_end_date=date(2016, 1, 1))
    approp = AppropriationFactory(
        allocation_transfer_agency=tas.allocation_transfer_agency,
        agency_identifier=tas.agency_identifier,
        beginning_period_of_availa=tas.beginning_period_of_availability,
        ending_period_of_availabil=tas.ending_period_of_availability,
        availability_type_code=tas.availability_type_code,
        main_account_code=tas.main_account_code,
        sub_account_code=tas.sub_account_code
    )
    submission = SubmissionFactory(
        reporting_start_date=date(2014, 1, 1),
        reporting_end_date=date(2014, 3, 31)
    )

    assert 1 == number_of_errors(
        _FILE, database, submission, models=[tas, approp])


@pytest.mark.parametrize('begin,end', [
    (date(2014, 12, 1), date(2015, 2, 1)),      # starts earlier
    (date(2015, 1, 1), date(2016, 1, 1)),       # exact overlap
    (date(2015, 1, 2), date(2015, 2, 1)),       # strict subset
    (date(2015, 12, 1), date(2016, 2, 1)),      # starts later
    (date(2014, 1, 1), date(2017, 1, 1))        # strict superset
])
def test_tas_in_span(database, begin, end):
    """If a TAS is open during a subset of an approp, it's still valid"""
    tas = TASFactory(internal_start_date=date(2015, 1, 1),
                     internal_end_date=date(2016, 1, 1))
    approp = AppropriationFactory(
        allocation_transfer_agency=tas.allocation_transfer_agency,
        agency_identifier=tas.agency_identifier,
        beginning_period_of_availa=tas.beginning_period_of_availability,
        ending_period_of_availabil=tas.ending_period_of_availability,
        availability_type_code=tas.availability_type_code,
        main_account_code=tas.main_account_code,
        sub_account_code=tas.sub_account_code
    )
    submission = SubmissionFactory(
        reporting_start_date=begin,
        reporting_end_date=end
    )

    assert 0 == number_of_errors(
        _FILE, database, submission, models=[tas, approp])
