from random import randint

from tests.unit.dataactcore.factories.domain import SF133Factory, TASFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import error_rows, number_of_errors, query_columns


_FILE = 'a33_appropriations_1'
_TAS = 'a33_appropriations_tas'


def test_column_headers(database):
    expected = {
        'uniqueid_TAS', 'row_number', 'allocation_transfer_agency', 'agency_identifier',
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
    cars_1 = TASFactory(financial_indicator2='other indicator')
    cars_2 = TASFactory(financial_indicator2=None)

    gtas_1 = SF133Factory(tas_id=cars_1.account_num, allocation_transfer_agency=None)

    gtas_2 = SF133Factory(tas_id=cars_2.account_num, period=gtas_1.period, fiscal_year=gtas_1.fiscal_year,
                          agency_identifier=gtas_1.agency_identifier, allocation_transfer_agency=None)

    submission_1 = SubmissionFactory(
        reporting_fiscal_period=gtas_1.period,
        reporting_fiscal_year=gtas_1.fiscal_year,
        cgac_code=gtas_1.agency_identifier
    )

    errors = number_of_errors(_FILE, database, models=[gtas_1, gtas_2, cars_1, cars_2], submission=submission_1)
    assert errors == 2

    cars_3 = TASFactory(financial_indicator2='f')
    cars_4 = TASFactory(financial_indicator2='F')

    gtas_3 = SF133Factory(tas_id=cars_3.account_num, allocation_transfer_agency=None)

    gtas_4 = SF133Factory(tas_id=cars_4.account_num, period=gtas_3.period, fiscal_year=gtas_3.fiscal_year,
                          agency_identifier=gtas_3.agency_identifier, allocation_transfer_agency=None)

    submission_2 = SubmissionFactory(
        reporting_fiscal_period=gtas_3.period,
        reporting_fiscal_year=gtas_3.fiscal_year,
        cgac_code=gtas_3.agency_identifier
    )

    errors = number_of_errors(_FILE, database, models=[gtas_3, gtas_4, cars_3, cars_4], submission=submission_2)
    assert errors == 0
