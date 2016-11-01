from datetime import date

import pytest

from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors

_FILE = 'a1_appropriations'

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
