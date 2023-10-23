from random import randint

from tests.unit.dataactcore.factories.domain import SF133Factory, TASFactory, CGACFactory, FRECFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import error_rows, number_of_errors, query_columns


_FILE = 'a33_appropriations_1'


def test_column_headers(database):
    expected = {
        'uniqueid_TAS', 'row_number', 'allocation_transfer_agency', 'agency_identifier',
        'beginning_period_of_availa', 'ending_period_of_availabil',
        'availability_type_code', 'main_account_code', 'sub_account_code',
    }
    assert expected == set(query_columns(_FILE, database))


def test_success_populated_ata_cgac(database):
    """ Tests that TAS for SF-133 are present in File A for CGAC submissions """
    submission_id = randint(1000, 10000)
    tas, period, year, code = 'some-tas', 2, 2002, 'some-code'

    sf1 = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=code,
                       agency_identifier='some-other-code')
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=code, frec_code=None)
    ap = AppropriationFactory(tas=tas, submission_id=submission_id)

    assert error_rows(_FILE, database, models=[sf1, ap], submission=submission) == []

    # accounting for CGAC 097, should still link with ata 021
    submission_id = randint(1000, 10000)
    tas, period, year, code = 'some-tas', 2, 2002, '097'

    sf1 = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency='021',
                       agency_identifier='some-other-code')
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=code, frec_code=None)
    ap = AppropriationFactory(tas=tas, submission_id=submission_id)

    assert error_rows(_FILE, database, models=[sf1, ap], submission=submission) == []


def test_failure_populated_ata_cgac(database):
    """ Tests that TAS for SF-133 are not present in File A for CGAC submissions """
    submission_id = randint(1000, 10000)
    tas, period, year, code = 'some-tas', 2, 2002, 'some-code'

    sf1 = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=code,
                       agency_identifier=code)
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=code, frec_code=None)
    ap = AppropriationFactory(tas='a-different-tas', submission_id=submission_id)

    errors = number_of_errors(_FILE, database, models=[sf1, ap], submission=submission)
    assert errors == 1

    # accounting for CGAC 097, should still link with ata 021
    submission_id = randint(1000, 10000)
    tas, period, year, code = 'some-tas', 2, 2002, '097'

    sf1 = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency='021',
                       agency_identifier='some-other-code')
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=code, frec_code=None)
    ap = AppropriationFactory(tas='a-different-tas', submission_id=submission_id)

    errors = number_of_errors(_FILE, database, models=[sf1, ap], submission=submission)
    assert errors == 1


def test_success_populated_ata_frec(database):
    """
        Tests that TAS for SF-133 are present in File A for FREC submissions
        Note this aspect of the filter is only relevant to FRECs 1601 and 1125
    """
    submission_id = randint(1000, 10000)
    tas, period, year, cgac_code, frec_code = 'some-tas', 2, 2002, '016', '1601'

    sf = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=cgac_code,
                      agency_identifier='some-other-code')
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=None, frec_code=frec_code)
    ap = AppropriationFactory(tas=tas, submission_id=submission_id)

    assert error_rows(_FILE, database, models=[sf, ap], submission=submission) == []

    submission_id = randint(1000, 10000)
    tas, period, year, cgac_code, frec_code = 'some-tas', 2, 2002, '011', '1125'

    sf = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=cgac_code,
                      agency_identifier='some-other-code')
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=None, frec_code=frec_code)
    ap = AppropriationFactory(tas=tas, submission_id=submission_id)

    assert error_rows(_FILE, database, models=[sf, ap], submission=submission) == []

    # testing with a not special FREC to show it'd still pass cause it doesn't link
    submission_id = randint(1000, 10000)
    tas, period, year, cgac_code, frec_code = 'some-tas', 2, 2002, '011', '1124'

    sf = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=cgac_code,
                      agency_identifier='some-other-code')
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=None, frec_code=frec_code)
    ap = AppropriationFactory(tas='a-different-tas', submission_id=submission_id)

    assert error_rows(_FILE, database, models=[sf, ap], submission=submission) == []


def test_failure_populated_ata_frec(database):
    """
        Tests that TAS for SF-133 are present in File A for FREC submissions
        Note this aspect of the filter is only relevant to FRECs 1601 and 1125
    """
    submission_id = randint(1000, 10000)
    tas, period, year, cgac_code, frec_code = 'some-tas', 2, 2002, '016', '1601'

    sf = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=cgac_code,
                      agency_identifier='some-other-code')
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=None, frec_code=frec_code)
    ap = AppropriationFactory(tas='a-different-tas', submission_id=submission_id)

    errors = number_of_errors(_FILE, database, models=[sf, ap], submission=submission)
    assert errors == 1

    submission_id = randint(1000, 10000)
    tas, period, year, cgac_code, frec_code = 'some-tas', 2, 2002, '011', '1125'

    sf = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=cgac_code,
                      agency_identifier='some-other-code')
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=None, frec_code=frec_code)
    ap = AppropriationFactory(tas='a-different-tas', submission_id=submission_id)

    errors = number_of_errors(_FILE, database, models=[sf, ap], submission=submission)
    assert errors == 1


def test_success_populated_aid_cgac(database):
    """ Tests that TAS for SF-133 are present in File A for CGAC submissions """
    submission_id = randint(1000, 10000)
    tas, period, year, code = 'some-tas', 2, 2002, 'some-code'

    sf1 = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=None,
                       agency_identifier=code)
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=code, frec_code=None)
    ap = AppropriationFactory(tas=tas, submission_id=submission_id)

    assert error_rows(_FILE, database, models=[sf1, ap], submission=submission) == []

    # accounting for CGAC 097, should still link with 017
    submission_id = randint(1000, 10000)
    tas, period, year, code = 'some-tas', 2, 2002, '097'

    sf1 = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=None,
                       agency_identifier='017')
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=code, frec_code=None)
    ap = AppropriationFactory(tas=tas, submission_id=submission_id)

    assert error_rows(_FILE, database, models=[sf1, ap], submission=submission) == []


def test_failure_populated_aid_cgac(database):
    """ Tests that TAS for SF-133 are not present in File A for CGAC submissions """
    submission_id = randint(1000, 10000)
    tas, period, year, code = 'some-tas', 2, 2002, 'some-code'

    sf1 = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=None,
                       agency_identifier=code)
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=code, frec_code=None)
    ap = AppropriationFactory(tas='a-different-tas', submission_id=submission_id)

    errors = number_of_errors(_FILE, database, models=[sf1, ap], submission=submission)
    assert errors == 1

    # accounting for CGAC 097, should still link with 017
    submission_id = randint(1000, 10000)
    tas, period, year, code = 'some-tas', 2, 2002, '097'

    sf1 = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=None,
                       agency_identifier='017')
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=code, frec_code=None)
    ap = AppropriationFactory(tas='a-different-tas', submission_id=submission_id)

    errors = number_of_errors(_FILE, database, models=[sf1, ap], submission=submission)
    assert errors == 1


def test_success_populated_aid_fr_entity_frec(database):
    """ Tests that TAS for SF-133 are present in File A for FREC submissions """
    submission_id = randint(1000, 10000)
    tas, account_num, period, year, cgac_code, frec_code = 'some-tas', 1, 2, 2002, 'some-cgac-code', 'some-frec-code'

    sf1 = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=None,
                       agency_identifier=cgac_code, account_num=account_num)
    ts1 = TASFactory(account_num=account_num, fr_entity_type=frec_code)
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=None, frec_code=frec_code)
    ap = AppropriationFactory(tas=tas, submission_id=submission_id)

    assert error_rows(_FILE, database, models=[sf1, ts1, ap], submission=submission) == []


def test_failure_populated_aid_fr_entity_frec(database):
    """ Tests that TAS for SF-133 are present in File A for FREC submissions """
    submission_id = randint(1000, 10000)
    tas, account_num, period, year, cgac_code, frec_code = 'some-tas', 1, 2, 2002, 'some-cgac-code', 'some-frec-code'

    sf1 = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=None,
                       agency_identifier=cgac_code, account_num=account_num)
    ts1 = TASFactory(account_num=account_num, fr_entity_type=frec_code, financial_indicator2='indicator')
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=None, frec_code=frec_code)
    ap = AppropriationFactory(tas='a-different-tas', submission_id=submission_id)

    errors = number_of_errors(_FILE, database, models=[sf1, ts1, ap], submission=submission)
    assert errors == 1


def test_success_populated_011_fr_entity_cgac(database):
    """ Tests that TAS for SF-133 are present in File A for CGAC submissions """
    submission_id = randint(1000, 10000)
    tas, account_num, period, year, cgac_code, frec_code = 'some-tas', 1, 2, 2002, 'some-cgac-code', 'some-frec-code'

    cgac = CGACFactory(cgac_code=cgac_code)
    frec = FRECFactory(cgac=cgac, frec_code=frec_code)
    sf1 = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=None,
                       agency_identifier='011', account_num=account_num)
    ts1 = TASFactory(account_num=account_num, fr_entity_type=frec_code)
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=cgac_code, frec_code=None)
    ap = AppropriationFactory(tas=tas, submission_id=submission_id)

    assert error_rows(_FILE, database, models=[cgac, frec, sf1, ts1, ap], submission=submission) == []


def test_failure_populated_011_fr_entity_cgac(database):
    """ Tests that TAS for SF-133 are present in File A for CGAC submissions """
    submission_id = randint(1000, 10000)
    tas, account_num, period, year, cgac_code, frec_code = 'some-tas', 1, 2, 2002, 'some-cgac-code', 'some-frec-code'

    cgac = CGACFactory(cgac_code=cgac_code)
    frec = FRECFactory(cgac=cgac, frec_code=frec_code)
    sf1 = SF133Factory(tas=tas, period=period, fiscal_year=year, allocation_transfer_agency=None,
                       agency_identifier='011', account_num=account_num)
    ts1 = TASFactory(account_num=account_num, fr_entity_type=frec_code)
    submission = SubmissionFactory(submission_id=submission_id, reporting_fiscal_period=period,
                                   reporting_fiscal_year=year, cgac_code=cgac_code, frec_code=None)
    ap = AppropriationFactory(tas='a-different-tas', submission_id=submission_id)

    errors = number_of_errors(_FILE, database, models=[cgac, frec, sf1, ts1, ap], submission=submission)
    assert errors == 1


def test_financing_tas(database):
    """GTAS entries associated with a CARS with a "financing" financial
    indicator should be ignored"""
    cars_1 = TASFactory(financial_indicator2='other indicator')
    cars_2 = TASFactory(financial_indicator2=None)

    gtas_1 = SF133Factory(account_num=cars_1.account_num, allocation_transfer_agency=None)

    gtas_2 = SF133Factory(account_num=cars_2.account_num, period=gtas_1.period, fiscal_year=gtas_1.fiscal_year,
                          agency_identifier=gtas_1.agency_identifier, allocation_transfer_agency=None)

    submission_1 = SubmissionFactory(
        reporting_fiscal_period=gtas_1.period,
        reporting_fiscal_year=gtas_1.fiscal_year,
        cgac_code=gtas_1.agency_identifier,
        frec_code=None
    )

    errors = number_of_errors(_FILE, database, models=[gtas_1, gtas_2, cars_1, cars_2], submission=submission_1)
    assert errors == 2

    cars_3 = TASFactory(financial_indicator2='f')
    cars_4 = TASFactory(financial_indicator2='F')

    gtas_3 = SF133Factory(account_num=cars_3.account_num, allocation_transfer_agency=None)

    gtas_4 = SF133Factory(account_num=cars_4.account_num, period=gtas_3.period, fiscal_year=gtas_3.fiscal_year,
                          agency_identifier=gtas_3.agency_identifier, allocation_transfer_agency=None)

    submission_2 = SubmissionFactory(
        reporting_fiscal_period=gtas_3.period,
        reporting_fiscal_year=gtas_3.fiscal_year,
        cgac_code=gtas_3.agency_identifier,
        frec_code=None
    )

    errors = number_of_errors(_FILE, database, models=[gtas_3, gtas_4, cars_3, cars_4], submission=submission_2)
    assert errors == 0
