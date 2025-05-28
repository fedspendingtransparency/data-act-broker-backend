from tests.unit.dataactcore.factories.domain import SF133Factory
from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = "a34_appropriations"
_TAS = "a34_appropriations_tas"


def test_column_headers(database):
    expected_subset = {
        "uniqueid_TAS",
        "row_number",
        "budget_authority_unobligat_fyb",
        "expected_value_GTAS SF133 Line 2490",
        "difference",
    }
    actual = set(query_columns(_FILE, database))
    assert actual == expected_subset


def test_success_fy2015(database):
    """Tests that SF-133 amount for line 2490 for the end of the last fiscal year equals Appropriation
    budget_authority_unobligat_fyb
    """

    sf = SF133Factory(line=2490, tas=_TAS, period=12, fiscal_year=2015, amount=1)
    ap = AppropriationFactory(tas=_TAS, budget_authority_unobligat_fyb=1)

    assert number_of_errors(_FILE, database, models=[sf, ap]) == 0


def test_success_fy2016(database):
    """If data for the end of the last fiscal year isn't present, the validation should still pass since it only looks
    for a specific fiscal year and period when executing the SQL
    """
    sf = SF133Factory(line=2490, tas=_TAS, period=12, fiscal_year=2016, amount=1)
    ap = AppropriationFactory(tas=_TAS, budget_authority_unobligat_fyb=0)

    assert number_of_errors(_FILE, database, models=[sf, ap]) == 0


def test_success_multi_line(database):
    """Tests that the validation is still successful if there are multiple SF-133 lines because of DEFC splits."""

    sf_1 = SF133Factory(line=2490, tas=_TAS, period=12, fiscal_year=2015, amount=1)
    sf_2 = SF133Factory(line=2490, tas=_TAS, period=12, fiscal_year=2015, amount=4)
    ap = AppropriationFactory(tas=_TAS, budget_authority_unobligat_fyb=5)

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, ap]) == 0


def test_failure(database):
    """Tests that SF-133 amount for line 2490 for the end of the last fiscal year does not equal Appropriation
    budget_authority_unobligat_fyb
    """

    sf = SF133Factory(line=2490, tas=_TAS, period=12, fiscal_year=2015, amount=1)
    ap = AppropriationFactory(tas=_TAS, budget_authority_unobligat_fyb=0)

    assert number_of_errors(_FILE, database, models=[sf, ap]) == 1


def test_failure_null(database):
    """Tests that SF-133 amount for line 2490 for the end of the last fiscal year does not equal Appropriation
    budget_authority_unobligat_fyb when budget_authority_unobligat_fyb is null
    """

    sf = SF133Factory(line=2490, tas=_TAS, period=12, fiscal_year=2015, amount=1)
    ap = AppropriationFactory(tas=_TAS, budget_authority_unobligat_fyb=None)

    assert number_of_errors(_FILE, database, models=[sf, ap]) == 1
