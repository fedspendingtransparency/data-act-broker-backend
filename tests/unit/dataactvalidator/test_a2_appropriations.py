from tests.unit.dataactcore.factories.domain import SF133Factory
from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "a2_appropriations"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "total_budgetary_resources_cpe",
        "budget_authority_appropria_cpe",
        "budget_authority_unobligat_fyb",
        "adjustments_to_unobligated_cpe",
        "other_budgetary_resources_cpe",
        "GTAS SF133 Line 1902",
        "difference",
        "uniqueid_TAS",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Test TotalBudgetaryResources_CPE = BudgetAuthorityAppropriatedAmount_CPE +
    BudgetAuthorityUnobligatedBalanceBroughtForward_FYB + AdjustmentsToUnobligatedBalanceBroughtForward_CPE +
    OtherBudgetaryResourcesAmount_CPE + SF 133 Line 1902
    """
    tas_1 = "tas_one_line"
    ap_1 = AppropriationFactory(
        total_budgetary_resources_cpe=1100,
        budget_authority_appropria_cpe=100,
        budget_authority_unobligat_fyb=200,
        adjustments_to_unobligated_cpe=300,
        other_budgetary_resources_cpe=400,
        display_tas=tas_1,
    )
    ap_null = AppropriationFactory(
        total_budgetary_resources_cpe=700,
        budget_authority_appropria_cpe=100,
        budget_authority_unobligat_fyb=200,
        adjustments_to_unobligated_cpe=300,
        other_budgetary_resources_cpe=None,
        display_tas=tas_1,
    )
    # Test with single SF133 line
    sf_1 = SF133Factory(line=1902, display_tas=tas_1, period=1, fiscal_year=2016, amount=100)

    # unrelated tas doesn't affect it
    sf_2 = SF133Factory(line=1902, display_tas="bcda", period=1, fiscal_year=2016, amount=200)

    # Different line in same TAS doesn't affect it
    sf_3 = SF133Factory(line=1900, display_tas=tas_1, period=1, fiscal_year=2016, amount=200)

    errors = number_of_errors(_FILE, database, models=[ap_1, ap_null, sf_1, sf_2, sf_3])
    assert errors == 0

    # Test with split SF133 lines
    tas_2 = "tas_two_lines"
    ap_2 = AppropriationFactory(
        total_budgetary_resources_cpe=1200,
        budget_authority_appropria_cpe=100,
        budget_authority_unobligat_fyb=200,
        adjustments_to_unobligated_cpe=300,
        other_budgetary_resources_cpe=400,
        display_tas=tas_2,
    )

    sf_4 = SF133Factory(line=1902, display_tas=tas_2, period=1, fiscal_year=2016, amount=100)
    sf_5 = SF133Factory(line=1902, display_tas=tas_2, period=1, fiscal_year=2016, amount=40, bea_category="A")
    sf_6 = SF133Factory(line=1902, display_tas=tas_2, period=1, fiscal_year=2016, amount=60, bea_category="B")

    assert number_of_errors(_FILE, database, models=[sf_4, sf_5, sf_6, ap_2]) == 0

    # Test with no 1902 line associated (this means it's a 0-value row and not in our DB, the other calculations
    # are good)
    ap_3 = AppropriationFactory(
        total_budgetary_resources_cpe=100,
        budget_authority_appropria_cpe=100,
        budget_authority_unobligat_fyb=0,
        adjustments_to_unobligated_cpe=0,
        other_budgetary_resources_cpe=0,
        display_tas="tas_no_sf",
    )

    assert number_of_errors(_FILE, database, models=[ap_3]) == 0


def test_failure(database):
    """Test failure TotalBudgetaryResources_CPE = BudgetAuthorityAppropriatedAmount_CPE +
    BudgetAuthorityUnobligatedBalanceBroughtForward_FYB + AdjustmentsToUnobligatedBalanceBroughtForward_CPE +
    OtherBudgetaryResourcesAmount_CPE + SF 133 Line 1902
    """

    approp = AppropriationFactory(
        total_budgetary_resources_cpe=1200,
        budget_authority_appropria_cpe=100,
        budget_authority_unobligat_fyb=200,
        adjustments_to_unobligated_cpe=300,
        other_budgetary_resources_cpe=400,
        display_tas="abcd",
    )
    approp_null = AppropriationFactory(
        total_budgetary_resources_cpe=800,
        budget_authority_appropria_cpe=100,
        budget_authority_unobligat_fyb=200,
        adjustments_to_unobligated_cpe=300,
        other_budgetary_resources_cpe=None,
        display_tas="abcd",
    )
    approp_wrong_tas = AppropriationFactory(
        total_budgetary_resources_cpe=1000,
        budget_authority_appropria_cpe=100,
        budget_authority_unobligat_fyb=200,
        adjustments_to_unobligated_cpe=300,
        other_budgetary_resources_cpe=400,
        display_tas="bcda",
    )

    # approp_wrong
    sf_1 = SF133Factory(line=1902, display_tas="bcda", period=1, fiscal_year=2016, amount=100)

    errors = number_of_errors(_FILE, database, models=[approp, approp_null, approp_wrong_tas, sf_1])
    assert errors == 3
