from random import randint

from tests.unit.dataactcore.factories.domain import SF133Factory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a33_appropriations_1'
_TAS = 'a33_appropriations_tas'


def test_column_headers(database):
    expected_subset = {'row_number', 'allocation_transfer_agency', 'agency_identifier',
                       'beginning_period_of_availa', 'ending_period_of_availabil',
                       'availability_type_code', 'main_account_code', 'sub_account_code'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that TAS for SF-133 are present in File A """
    submission_id = randint(1000, 10000)
    tas, period, year, code = 'some-tas', 2, 2002, 'some-code'
    sf1 = SF133Factory(tas=tas, period=period, fiscal_year=year,
                       agency_identifier=code)
    sf2 = SF133Factory(tas=tas, period=period, fiscal_year=year,
                       agency_identifier=code)
    submission = SubmissionFactory(
        submission_id=submission_id, reporting_fiscal_period=period,
        reporting_fiscal_year=year, cgac_code=code)
    ap = AppropriationFactory(tas=tas, submission_id=submission_id)

    errors = number_of_errors(_FILE, database,
                              models=[sf1, sf2, ap], submission=submission)
    assert errors == 0


def test_failure(database):
    """ Tests that TAS for SF-133 are not present in File A """
    submission_id = randint(1000, 10000)
    tas, period, year, code = 'some-tas', 2, 2002, 'some-code'
    sf1 = SF133Factory(tas=tas, period=period, fiscal_year=year,
                       agency_identifier=code)
    sf2 = SF133Factory(tas=tas, period=period, fiscal_year=year,
                       agency_identifier=code)
    submission = SubmissionFactory(
        submission_id=submission_id, reporting_fiscal_period=period,
        reporting_fiscal_year=year, cgac_code=code)
    ap = AppropriationFactory(tas='a-different-tas',
                              submission_id=submission_id)

    errors = number_of_errors(_FILE, database,
                              models=[sf1, sf2, ap], submission=submission)
    assert errors == 2
