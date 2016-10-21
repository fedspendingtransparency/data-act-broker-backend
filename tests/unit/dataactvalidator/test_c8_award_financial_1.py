from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactcore.factories.domain import CGACFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'c8_award_financial_1'
_TAS = 'c8_award_financial_tas'


def test_column_headers(database):
    expected_subset = {'row_number', 'fain', 'uri'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_equal_fain(database):
    """Tests that File C (award financial) fain matches
    File D2 (award financial assistance) fain."""
    tas = _TAS
    af = AwardFinancialFactory(
        tas=tas, fain='abc', uri=None, allocation_transfer_agency=None)

    errors = number_of_errors(_FILE, database, models=[af])
    assert errors == 0


def test_equal_uri(database):
    """Tests that File C (award financial) uri matches
    File D2 (award financial assistance) uri."""
    tas = _TAS
    af = AwardFinancialFactory(
        tas=tas, fain=None, uri='xyz', allocation_transfer_agency=None)

    errors = number_of_errors(_FILE, database, models=[af])
    assert errors == 0


def test_null_uri_fain(database):
    """Tests File C (award financial) and File D2 (award financial assistance)
    having NULL values for both fain and uri."""
    tas = _TAS
    af = AwardFinancialFactory(
        tas=tas, fain=None, uri=None, allocation_transfer_agency=None)

    errors = number_of_errors(_FILE, database, models=[af])
    assert errors == 0


def test_both_fain_and_url_supplied(database):
    """Tests File C (award financial) having both uri and fain populated ."""
    tas = _TAS
    af = AwardFinancialFactory(
        tas=tas, fain='abc', uri='xyz', allocation_transfer_agency=None)

    errors = number_of_errors(_FILE, database, models=[af])
    assert errors == 1


def test_invalid_allocation_transfer_agency(database):
    """Tests that validation processes when there's an invalid allocation
    transfer agency in File C (award financial). Per rule C24."""
    tas = _TAS
    cgac = CGACFactory(cgac_code='good')
    af = AwardFinancialFactory(
        tas=tas, fain='abc', uri='xyz', allocation_transfer_agency='bad')

    errors = number_of_errors(_FILE, database, models=[af, cgac])
    assert errors == 1


def test_valid_allocation_transfer_agency(database):
    """Tests that rule is not applied when File C (award financial)
    record has a valid allocation transfer agency."""
    tas = _TAS
    cgac = CGACFactory(cgac_code='good')
    af = AwardFinancialFactory(
        tas=tas, fain='abc', uri='xyz', allocation_transfer_agency=cgac.cgac_code)

    errors = number_of_errors(_FILE, database, models=[af, cgac])
    assert errors == 0




