from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = "b29_award_financial_2"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "prior_year_adjustment",
        "uniqueid_TAS",
        "uniqueid_DisasterEmergencyFundCode",
        "uniqueid_PIID",
        "uniqueid_FAIN",
        "uniqueid_URI",
    }
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success_no_pya(database):
    """If PYA is provided in File C, it must be on all (non-TOA) balance rows. No PYA in file."""

    # No PYAs in file
    af1 = AwardFinancialFactory(prior_year_adjustment="", transaction_obligated_amou=5)
    af2 = AwardFinancialFactory(prior_year_adjustment=None, transaction_obligated_amou=0)
    af3 = AwardFinancialFactory(prior_year_adjustment=None, transaction_obligated_amou=None)

    assert number_of_errors(_FILE, database, models=[af1, af2, af3]) == 0


def test_success_pya_on_all(database):
    """If PYA is provided in File C, it must be on all (non-TOA) balance rows. All have PYA."""

    # Both contain data
    af1 = AwardFinancialFactory(prior_year_adjustment="X", transaction_obligated_amou=5)
    af2 = AwardFinancialFactory(prior_year_adjustment="X", transaction_obligated_amou=None)

    # This is not a non-TOA row, so it is ignored for the purpose of the rule
    af3 = AwardFinancialFactory(prior_year_adjustment=None, transaction_obligated_amou=0)

    assert number_of_errors(_FILE, database, models=[af1, af2, af3]) == 0


def test_failure(database):
    """Tests failure if PYA is provided in File C, it must be on all (non-TOA) balance rows."""

    af1 = AwardFinancialFactory(prior_year_adjustment="X", transaction_obligated_amou=5)

    # Both blank string and null are failures
    af2 = AwardFinancialFactory(prior_year_adjustment="", transaction_obligated_amou=None)
    af3 = AwardFinancialFactory(prior_year_adjustment=None, transaction_obligated_amou=None)

    assert number_of_errors(_FILE, database, models=[af1, af2, af3]) == 2
