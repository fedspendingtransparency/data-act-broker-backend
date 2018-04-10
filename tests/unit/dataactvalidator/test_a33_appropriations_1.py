from random import randint

from tests.unit.dataactcore.factories.domain import SF133Factory, TASFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import error_rows, number_of_errors, query_columns


_FILE = 'a33_appropriations_1'
_TAS = 'a33_appropriations_tas'


def test_column_headers(database):
    expected = {
        'row_number', 'allocation_transfer_agency', 'agency_identifier',
        'beginning_period_of_availa', 'ending_period_of_availabil',
        'availability_type_code', 'main_account_code', 'sub_account_code',
    }
    assert expected == set(query_columns(_FILE, database))


def test_success_populated_ata(database):
    """ Tests that TAS for SF-133 are present in File A """
    submission_id = randint(1000, 10000)
    tas, period, year, code = 'some-tas', 2, 2002, 'some-code'

    sf1 = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=code,
                       agency_identifier='some-other-code')
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=code)
    ap = AppropriationFactory(tas=tas, submission_id=submission_id)

    assert error_rows(_FILE, database, models=[sf1, ap], submission=submission) == []


def test_success_null_ata(database):
    """ Tests that TAS for SF-133 are present in File A """
    submission_id = randint(1000, 10000)
    tas, period, year, code = 'some-tas', 2, 2002, 'some-code'

    sf1 = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=None,
                       agency_identifier=code)
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=code)
    ap = AppropriationFactory(tas=tas, submission_id=submission_id)

    assert error_rows(_FILE, database, models=[sf1, ap], submission=submission) == []


def test_failure_populated_ata(database):
    """ Tests that TAS for SF-133 are not present in File A """
    submission_id = randint(1000, 10000)
    tas, period, year, code = 'some-tas', 2, 2002, 'some-code'

    sf1 = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=code,
                       agency_identifier=code)
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=code)
    ap = AppropriationFactory(tas='a-different-tas', submission_id=submission_id)

    errors = number_of_errors(_FILE, database, models=[sf1, ap], submission=submission)
    assert errors == 1


def test_failure_null_ata(database):
    """ Tests that TAS for SF-133 are not present in File A """
    submission_id = randint(1000, 10000)
    tas, period, year, code = 'some-tas', 2, 2002, 'some-code'

    sf1 = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=None,
                       agency_identifier=code)
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=code)
    ap = AppropriationFactory(tas='a-different-tas', submission_id=submission_id)

    errors = number_of_errors(_FILE, database, models=[sf1, ap], submission=submission)
    assert errors == 1


def test_financing_tas(database):
    """GTAS entries associated with a CARS with a "financing" financial
    indicator should be ignored"""
    cars = TASFactory()
    database.session.add(cars)
    database.session.commit()
    gtas = SF133Factory(tas_id=cars.account_num)
    submission = SubmissionFactory(
        reporting_fiscal_period=gtas.period,
        reporting_fiscal_year=gtas.fiscal_year,
        cgac_code=gtas.allocation_transfer_agency
    )
    errors = number_of_errors(_FILE, database, models=[gtas, cars], submission=submission)
    assert errors == 1

    cars.financial_indicator2 = 'F'
    assert error_rows(_FILE, database, models=[gtas, cars], submission=submission) == []
