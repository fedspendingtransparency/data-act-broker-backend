from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactcore.factories.staging import AwardFinancialAssistanceFactory
from tests.unit.dataactcore.factories.domain import CGACFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'c8_award_financial'
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
    afa = AwardFinancialAssistanceFactory(
        tas=tas, submisson_id=af.submission_id, fain=af.fain, uri=None)

    errors = number_of_errors(_FILE, database, models=[af, afa])
    assert errors == 0


def test_equal_uri(database):
    """Tests that File C (award financial) uri matches
    File D2 (award financial assistance) uri."""
    tas = _TAS
    af = AwardFinancialFactory(
        tas=tas, fain=None, uri='xyz', allocation_transfer_agency=None)
    afa = AwardFinancialAssistanceFactory(
        tas=tas, submisson_id=af.submission_id, fain=None, uri=af.uri)

    errors = number_of_errors(_FILE, database, models=[af, afa])
    assert errors == 0


def test_null_uri_fain(database):
    """Tests File C (award financial) and File D2 (award financial assistance)
    having NULL values for both fain and uri."""
    tas = _TAS
    af = AwardFinancialFactory(
        tas=tas, fain=None, uri=None, allocation_transfer_agency=None)
    afa = AwardFinancialAssistanceFactory(
        tas=tas, submission_id=af.submission_id, fain=None, uri=None)

    errors = number_of_errors(_FILE, database, models=[af, afa])
    assert errors == 0


def test_equal_uri_fain(database):
    """Tests File C (award financial) and File D2 (award financial assistance)
    having both uri and fain populated and equal between the files."""
    tas = _TAS
    af = AwardFinancialFactory(
        tas=tas, fain='abc', uri='xyz', allocation_transfer_agency=None)
    afa = AwardFinancialAssistanceFactory(
        tas=tas, submission_id=af.submission_id, fain=af.fain, uri=af.uri)

    errors = number_of_errors(_FILE, database, models=[af, afa])
    assert errors == 0


def test_unequal_fain(database):
    """Tests File C (award financial) fain different than File D2
    (award financial assistance) fain."""
    tas = _TAS
    af = AwardFinancialFactory(
        tas=tas, fain='abc', uri=None, allocation_transfer_agency=None)
    afa = AwardFinancialAssistanceFactory(
        tas=tas, submission_id=af.submission_id, fain='xyz', uri=None)

    errors = number_of_errors(_FILE, database, models=[af, afa])
    assert errors == 1


def test_unequal_uri(database):
    """Tests File C (award financial) uri different than File D2
    (award financial assistance) fain."""
    tas = _TAS
    af = AwardFinancialFactory(
        tas=tas, fain=None, uri='abc', allocation_transfer_agency=None)
    afa = AwardFinancialAssistanceFactory(
        tas=tas, submission_id=af.submission_id, fain=None, uri='xyz')

    errors = number_of_errors(_FILE, database, models=[af, afa])
    assert errors == 1


def test_unequal_fain_null(database):
    """Tests non-NULL File C (award financial) fain compared to a NULL
    fain in File D2 (award financial assistance)."""
    tas = _TAS
    af = AwardFinancialFactory(
        tas=tas, fain='abc', uri=None, allocation_transfer_agency=None)
    afa = AwardFinancialAssistanceFactory(
        tas=tas, submission_id=af.submission_id, fain=None, uri=None)

    errors = number_of_errors(_FILE, database, models=[af, afa])
    assert errors == 1


def test_unequal_uri_null(database):
    """Tests NULL File C (award financial) uri compared to a non-NULL
    uri in File D2 (award financial assistance)."""
    tas = _TAS
    af = AwardFinancialFactory(
        tas=tas, fain=None, uri=None, allocation_transfer_agency=None)
    afa = AwardFinancialAssistanceFactory(
        tas=tas, submission_id=af.submission_id, fain=None, uri='abc')

    errors = number_of_errors(_FILE, database, models=[af, afa])
    assert errors == 1


def test_equal_fain_unequal_uri(database):
    """Tests equal fain and unequal uri values between File C
    (award financial) and File D2 (award financial assistance)."""
    tas = _TAS
    af = AwardFinancialFactory(
        tas=tas, fain='abc', uri=None, allocation_transfer_agency=None)
    afa = AwardFinancialAssistanceFactory(
        tas=tas, submission_id=af.submission_id, fain=af.fain, uri='xyz')

    errors = number_of_errors(_FILE, database, models=[af, afa])
    assert errors == 1


def test_unequal_fain_equal_uri(database):
    """Tests equal fain and unequal uri values between File C
    (award financial) and File D2 (award financial assistance)."""
    tas = _TAS
    af = AwardFinancialFactory(
        tas=tas, fain='abc', uri='xyz', allocation_transfer_agency=None)
    afa = AwardFinancialAssistanceFactory(
        tas=tas, submission_id=af.submission_id, fain='bad', uri=af.uri)

    errors = number_of_errors(_FILE, database, models=[af, afa])
    assert errors == 1


def test_invalid_allocation_transfer_agency(database):
    """Tests that validation processes when there's an invalid allocation
    transfer agency in File C (award financial). Per rule C24."""
    tas = _TAS
    cgac = CGACFactory(cgac_code='good')
    af = AwardFinancialFactory(
        tas=tas, fain='abc', uri='xyz', allocation_transfer_agency='bad')
    afa = AwardFinancialAssistanceFactory(
        tas=tas, submission_id=af.submission_id, fain='123', uri='456')

    errors = number_of_errors(_FILE, database, models=[af, afa, cgac])
    assert errors == 1


def test_valid_allocation_transfer_agency(database):
    """Tests that rule is not applied when File C (award financial)
    record has a valid allocation transfer agency."""
    tas = _TAS
    cgac = CGACFactory(cgac_code='good')
    af = AwardFinancialFactory(
        tas=tas, fain='abc', uri='xyz', allocation_transfer_agency=cgac.cgac_code)
    afa = AwardFinancialAssistanceFactory(
        tas=tas, submission_id=af.submission_id, fain='123', uri='456')

    errors = number_of_errors(_FILE, database, models=[af, afa, cgac])
    assert errors == 0




