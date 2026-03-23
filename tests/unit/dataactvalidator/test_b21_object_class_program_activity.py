from random import randint

from tests.unit.dataactcore.factories.domain import SF133Factory, TASFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = "b21_object_class_program_activity"


def test_column_headers(database):
    expected = {
        "uniqueid_TAS",
        "uniqueid_DisasterEmergencyFundCode",
        "row_number",
        "allocation_transfer_agency",
        "agency_identifier",
        "beginning_period_of_availa",
        "ending_period_of_availabil",
        "availability_type_code",
        "main_account_code",
        "sub_account_code",
        "disaster_emergency_fund_code",
    }
    assert expected == set(query_columns(_FILE, database))


def test_success_populated_ata(database):
    """Tests that TAS/DEFC for SF-133 are present in File B"""
    submission_id = randint(1000, 10000)
    tas, period, year, code = "some-tas", 2, 2002, "some-code"

    sf1 = SF133Factory(
        display_tas=tas,
        period=period,
        fiscal_year=year,
        allocation_transfer_agency=code,
        agency_identifier="some-other-code",
        disaster_emergency_fund_code="N",
        line=3020,
        amount=4,
        bea_category="a",
    )
    sf2 = SF133Factory(
        display_tas=tas,
        period=period,
        fiscal_year=year,
        allocation_transfer_agency=code,
        agency_identifier="some-other-code",
        disaster_emergency_fund_code="N",
        line=3020,
        amount=4,
        bea_category="b",
    )
    submission = SubmissionFactory(
        submission_id=submission_id,
        reporting_fiscal_period=period,
        reporting_fiscal_year=year,
        cgac_code=code,
        is_quarter_format=False,
    )
    op = ObjectClassProgramActivityFactory(
        display_tas=tas, disaster_emergency_fund_code="n", submission_id=submission_id, prior_year_adjustment="x"
    )

    errors = number_of_errors(_FILE, database, models=[sf1, sf2, op], submission=submission)
    assert errors == 0


def test_success_null_ata(database):
    """Tests that TAS/DEFC for SF-133 are present in File B (null ATA)"""
    submission_id = randint(1000, 10000)
    tas, period, year, code = "some-tas", 2, 2002, "some-code"

    sf1 = SF133Factory(
        display_tas=tas,
        period=period,
        fiscal_year=year,
        allocation_transfer_agency=None,
        agency_identifier=code,
        disaster_emergency_fund_code="N",
        line=2190,
        amount=4,
    )
    submission = SubmissionFactory(
        submission_id=submission_id,
        reporting_fiscal_period=period,
        reporting_fiscal_year=year,
        cgac_code=code,
        is_quarter_format=False,
    )
    op = ObjectClassProgramActivityFactory(
        display_tas=tas, disaster_emergency_fund_code="n", submission_id=submission_id, prior_year_adjustment="X"
    )

    errors = number_of_errors(_FILE, database, models=[sf1, op], submission=submission)
    assert errors == 0


def test_success_ignore_lines(database):
    """Tests lines that should be ignored in SF133 are"""
    submission_id = randint(1000, 10000)
    tas, period, year, code = "some-tas", 2, 2002, "some-code"

    sf1 = SF133Factory(
        display_tas=tas,
        period=period,
        fiscal_year=year,
        allocation_transfer_agency=code,
        agency_identifier="some-other-code",
        disaster_emergency_fund_code="N",
        line=3020,
        amount=4,
    )
    # Invalid line number
    sf2 = SF133Factory(
        display_tas=tas,
        period=period,
        fiscal_year=year,
        allocation_transfer_agency=code,
        agency_identifier="some-other-code",
        disaster_emergency_fund_code="M",
        line=1000,
        amount=4,
    )
    # amount of 0
    sf3 = SF133Factory(
        display_tas=tas,
        period=period,
        fiscal_year=year,
        allocation_transfer_agency=code,
        agency_identifier="some-other-code",
        disaster_emergency_fund_code="L",
        line=3020,
        amount=0,
    )
    submission = SubmissionFactory(
        submission_id=submission_id,
        reporting_fiscal_period=period,
        reporting_fiscal_year=year,
        cgac_code=code,
        is_quarter_format=False,
    )
    op = ObjectClassProgramActivityFactory(
        display_tas=tas, disaster_emergency_fund_code="n", submission_id=submission_id, prior_year_adjustment="X"
    )
    # Different PYA
    op2 = ObjectClassProgramActivityFactory(
        display_tas=tas, disaster_emergency_fund_code="n", submission_id=submission_id, prior_year_adjustment="A"
    )

    errors = number_of_errors(_FILE, database, models=[sf1, sf2, sf3, op, op2], submission=submission)
    assert errors == 0


def test_success_no_sf(database):
    """Tests that lines that don't have an sf133 (0-value) don't throw an error"""
    submission_id = randint(1000, 10000)
    tas, period, year, code = "tas-no-sf", 2, 2002, "some-code"

    submission = SubmissionFactory(
        submission_id=submission_id,
        reporting_fiscal_period=period,
        reporting_fiscal_year=year,
        cgac_code=code,
        is_quarter_format=False,
    )
    op = ObjectClassProgramActivityFactory(
        display_tas=tas, disaster_emergency_fund_code="n", submission_id=submission_id, prior_year_adjustment="X"
    )

    errors = number_of_errors(_FILE, database, models=[op], submission=submission)
    assert errors == 0


def test_failure_populated_ata(database):
    """Tests that TAS/DEFC for SF-133 are not present in File B"""
    submission_id = randint(1000, 10000)
    tas, period, year, code = "some-tas", 2, 2002, "some-code"

    sf1 = SF133Factory(
        display_tas=tas,
        period=period,
        fiscal_year=year,
        allocation_transfer_agency=code,
        agency_identifier=code,
        disaster_emergency_fund_code="N",
        line=3020,
        amount=4,
    )
    submission = SubmissionFactory(
        submission_id=submission_id,
        reporting_fiscal_period=period,
        reporting_fiscal_year=year,
        cgac_code=code,
        is_quarter_format=False,
    )
    op = ObjectClassProgramActivityFactory(
        display_tas="a-different-tas",
        submission_id=submission_id,
        disaster_emergency_fund_code="n",
        prior_year_adjustment="x",
    )

    errors = number_of_errors(_FILE, database, models=[sf1, op], submission=submission)
    assert errors == 1


def test_failure_null_ata(database):
    """Tests that TAS/DEFC for SF-133 are not present in File B (null ATA)"""
    submission_id = randint(1000, 10000)
    tas, period, year, code = "some-tas", 2, 2002, "some-code"

    sf1 = SF133Factory(
        display_tas=tas,
        period=period,
        fiscal_year=year,
        allocation_transfer_agency=None,
        agency_identifier=code,
        disaster_emergency_fund_code="N",
        line=3020,
        amount=4,
    )
    submission = SubmissionFactory(
        submission_id=submission_id,
        reporting_fiscal_period=period,
        reporting_fiscal_year=year,
        cgac_code=code,
        is_quarter_format=False,
    )
    op = ObjectClassProgramActivityFactory(
        display_tas="a-different-tas",
        submission_id=submission_id,
        disaster_emergency_fund_code="n",
        prior_year_adjustment="X",
    )

    errors = number_of_errors(_FILE, database, models=[sf1, op], submission=submission)
    assert errors == 1


def test_financing_tas(database):
    """GTAS entries associated with a CARS with a "financing" financial indicator should be ignored"""
    cars_1 = TASFactory(financial_indicator2="other indicator")
    cars_2 = TASFactory(financial_indicator2=None)

    gtas_1 = SF133Factory(
        account_num=cars_1.account_num,
        allocation_transfer_agency=None,
        disaster_emergency_fund_code="N",
        line=3020,
        amount=4,
    )
    gtas_2 = SF133Factory(
        account_num=cars_2.account_num,
        period=gtas_1.period,
        fiscal_year=gtas_1.fiscal_year,
        agency_identifier=gtas_1.agency_identifier,
        allocation_transfer_agency=None,
        disaster_emergency_fund_code="N",
        line=3020,
        amount=4,
    )

    submission = SubmissionFactory(
        reporting_fiscal_period=gtas_1.period,
        reporting_fiscal_year=gtas_1.fiscal_year,
        cgac_code=gtas_1.agency_identifier,
        is_quarter_format=False,
    )

    errors = number_of_errors(_FILE, database, models=[gtas_1, gtas_2, cars_1, cars_2], submission=submission)
    assert errors == 2

    cars_3 = TASFactory(financial_indicator2="f")
    cars_4 = TASFactory(financial_indicator2="F")

    gtas_3 = SF133Factory(
        account_num=cars_3.account_num, allocation_transfer_agency=None, disaster_emergency_fund_code="N"
    )
    gtas_4 = SF133Factory(
        account_num=cars_4.account_num,
        period=gtas_3.period,
        fiscal_year=gtas_3.fiscal_year,
        agency_identifier=gtas_3.agency_identifier,
        allocation_transfer_agency=None,
        disaster_emergency_fund_code="N",
    )

    submission = SubmissionFactory(
        reporting_fiscal_period=gtas_3.period,
        reporting_fiscal_year=gtas_3.fiscal_year,
        cgac_code=gtas_3.agency_identifier,
        is_quarter_format=False,
    )

    errors = number_of_errors(_FILE, database, models=[gtas_3, gtas_4, cars_3, cars_4], submission=submission)
    assert errors == 0


def test_ignore_quarterly_submissions(database):
    """Tests that rule doesn't apply to quarterly submissions"""
    submission_id = randint(1000, 10000)
    tas, period, year, code = "some-tas", 2, 2002, "some-code"

    sf1 = SF133Factory(
        display_tas=tas,
        period=period,
        fiscal_year=year,
        allocation_transfer_agency=code,
        agency_identifier=code,
        disaster_emergency_fund_code="N",
        line=3020,
        amount=4,
    )
    submission = SubmissionFactory(
        submission_id=submission_id,
        reporting_fiscal_period=period,
        reporting_fiscal_year=year,
        cgac_code=code,
        is_quarter_format=True,
    )
    op = ObjectClassProgramActivityFactory(
        display_tas="a-different-tas",
        submission_id=submission_id,
        disaster_emergency_fund_code="n",
        prior_year_adjustment="X",
    )

    errors = number_of_errors(_FILE, database, models=[sf1, op], submission=submission)
    assert errors == 0


def test_non_matching_defc(database):
    """Tests that even if TAS matches, if DEFC doesn't it throws an error"""
    submission_id = randint(1000, 10000)
    tas, period, year, code = "some-tas", 2, 2002, "some-code"

    sf1 = SF133Factory(
        display_tas=tas,
        period=period,
        fiscal_year=year,
        allocation_transfer_agency=code,
        agency_identifier="some-other-code",
        disaster_emergency_fund_code="N",
        line=3020,
        amount=4,
    )
    submission = SubmissionFactory(
        submission_id=submission_id,
        reporting_fiscal_period=period,
        reporting_fiscal_year=year,
        cgac_code=code,
        is_quarter_format=False,
    )
    op = ObjectClassProgramActivityFactory(
        display_tas=tas, disaster_emergency_fund_code="m", submission_id=submission_id, prior_year_adjustment="x"
    )

    errors = number_of_errors(_FILE, database, models=[sf1, op], submission=submission)
    assert errors == 1
