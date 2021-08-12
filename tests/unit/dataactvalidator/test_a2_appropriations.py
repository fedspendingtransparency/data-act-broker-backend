from tests.unit.dataactcore.factories.domain import SF133Factory
from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'a2_appropriations'


def test_column_headers(database):
    expected_subset = {'uniqueid_TAS', 'row_number', 'total_budgetary_resources_cpe', 'budget_authority_appropria_cpe',
                       'budget_authority_unobligat_fyb', 'adjustments_to_unobligated_cpe',
                       'other_budgetary_resources_cpe', 'difference'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test TotalBudgetaryResources_CPE = BudgetAuthorityAppropriatedAmount_CPE +
        BudgetAuthorityUnobligatedBalanceBroughtForward_FYB + AdjustmentsToUnobligatedBalanceBroughtForward_CPE +
        OtherBudgetaryResourcesAmount_CPE + SF 133 Line 1902
    """
    approp = AppropriationFactory(total_budgetary_resources_cpe=1100, budget_authority_appropria_cpe=100,
                                  budget_authority_unobligat_fyb=200, adjustments_to_unobligated_cpe=300,
                                  other_budgetary_resources_cpe=400, tas='abcd')
    approp_null = AppropriationFactory(total_budgetary_resources_cpe=700, budget_authority_appropria_cpe=100,
                                       budget_authority_unobligat_fyb=200, adjustments_to_unobligated_cpe=300,
                                       other_budgetary_resources_cpe=None, tas='abcd')
    sf_1 = SF133Factory(line=1902, tas='abcd', period=1, fiscal_year=2016, amount=100)

    # unrelated tas doesn't affect it
    sf_2 = SF133Factory(line=1902, tas='bcda', period=1, fiscal_year=2016, amount=200)

    # Different line in same TAS doesn't affect it
    sf_3 = SF133Factory(line=1900, tas='abcd', period=1, fiscal_year=2016, amount=200)

    errors = number_of_errors(_FILE, database, models=[approp, approp_null, sf_1, sf_2, sf_3])
    assert errors == 0


def test_failure(database):
    """ Test failure TotalBudgetaryResources_CPE = BudgetAuthorityAppropriatedAmount_CPE +
        BudgetAuthorityUnobligatedBalanceBroughtForward_FYB + AdjustmentsToUnobligatedBalanceBroughtForward_CPE +
        OtherBudgetaryResourcesAmount_CPE + SF 133 Line 1902
    """

    approp = AppropriationFactory(total_budgetary_resources_cpe=1200, budget_authority_appropria_cpe=100,
                                  budget_authority_unobligat_fyb=200, adjustments_to_unobligated_cpe=300,
                                  other_budgetary_resources_cpe=400, tas='abcd')
    approp_null = AppropriationFactory(total_budgetary_resources_cpe=800, budget_authority_appropria_cpe=100,
                                       budget_authority_unobligat_fyb=200, adjustments_to_unobligated_cpe=300,
                                       other_budgetary_resources_cpe=None, tas='abcd')
    approp_wrong_tas = AppropriationFactory(total_budgetary_resources_cpe=1000, budget_authority_appropria_cpe=100,
                                            budget_authority_unobligat_fyb=200, adjustments_to_unobligated_cpe=300,
                                            other_budgetary_resources_cpe=400, tas='bcda')

    # approp_wrong
    sf_1 = SF133Factory(line=1902, tas='bcda', period=1, fiscal_year=2016, amount=100)

    errors = number_of_errors(_FILE, database, models=[approp, approp_null, approp_wrong_tas, sf_1])
    assert errors == 3
