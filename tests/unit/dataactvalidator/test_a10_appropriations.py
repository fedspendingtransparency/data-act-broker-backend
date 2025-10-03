from tests.unit.dataactcore.factories.domain import SF133Factory
from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = "a10_appropriations"
_TAS = "a10_appropriations_tas"


def test_column_headers(database):
    expected_subset = {
        "uniqueid_TAS",
        "row_number",
        "borrowing_authority_amount_cpe",
        "expected_value_SUM of GTAS SF133 Lines 1340, 1440",
        "difference",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Tests that SF 133 amount sum for lines 1340, 1440 matches Appropriation borrowing_authority_amount_cpe for the
    specified fiscal year and period
    """
    tas_1 = "".join([_TAS, "_success"])
    tas_2 = "".join([_TAS, "_success_2"])

    sf_1 = SF133Factory(line=1340, display_tas=tas_1, period=1, fiscal_year=2016, amount=1)
    sf_2 = SF133Factory(line=1440, display_tas=tas_1, period=1, fiscal_year=2016, amount=1)
    sf_3 = SF133Factory(line=1340, display_tas=tas_2, period=1, fiscal_year=2016, amount=0)
    sf_4 = SF133Factory(line=1440, display_tas=tas_2, period=1, fiscal_year=2016, amount=0)
    ap_1 = AppropriationFactory(display_tas=tas_1, borrowing_authority_amount_cpe=2)
    ap_2 = AppropriationFactory(display_tas=tas_2, borrowing_authority_amount_cpe=None)

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, sf_3, sf_4, ap_1, ap_2]) == 0


def test_failure(database):
    """Tests that SF 133 amount sum for lines 1340, 1440 does not match Appropriation borrowing_authority_amount_cpe
    for the specified fiscal year and period
    """
    tas = "".join([_TAS, "_failure"])

    sf_1 = SF133Factory(line=1340, display_tas=tas, period=1, fiscal_year=2016, amount=1)
    sf_2 = SF133Factory(line=1440, display_tas=tas, period=1, fiscal_year=2016, amount=1)
    ap_1 = AppropriationFactory(display_tas=tas, borrowing_authority_amount_cpe=1)
    ap_2 = AppropriationFactory(display_tas=tas, borrowing_authority_amount_cpe=None)

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, ap_1, ap_2]) == 2
