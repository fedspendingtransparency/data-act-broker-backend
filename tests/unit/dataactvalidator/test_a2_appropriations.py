from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'a2_appropriations'


def test_column_headers(database):
    expected_subset = {'row_number', 'total_budgetary_resources_cpe', 'budget_authority_appropria_cpe',
                       'budget_authority_unobligat_fyb', 'adjustments_to_unobligated_cpe',
                       'other_budgetary_resources_cpe', 'difference'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test that TAS values can be found, and null matches work correctly """
    approp = AppropriationFactory(total_budgetary_resources_cpe=1000, budget_authority_appropria_cpe=100,
                                  budget_authority_unobligat_fyb=200, adjustments_to_unobligated_cpe=300,
                                  other_budgetary_resources_cpe=400)
    approp_null = AppropriationFactory(total_budgetary_resources_cpe=600, budget_authority_appropria_cpe=100,
                                       budget_authority_unobligat_fyb=200, adjustments_to_unobligated_cpe=300,
                                       other_budgetary_resources_cpe=None)

    errors = number_of_errors(_FILE, database, models=[approp, approp_null])
    assert errors == 0


def test_failure(database):
    """ Test that tas that does not match is an error """

    approp = AppropriationFactory(total_budgetary_resources_cpe=1200, budget_authority_appropria_cpe=100,
                                  budget_authority_unobligat_fyb=200, adjustments_to_unobligated_cpe=300,
                                  other_budgetary_resources_cpe=400)
    approp_null = AppropriationFactory(total_budgetary_resources_cpe=800, budget_authority_appropria_cpe=100,
                                       budget_authority_unobligat_fyb=200, adjustments_to_unobligated_cpe=300,
                                       other_budgetary_resources_cpe=None)

    errors = number_of_errors(_FILE, database, models=[approp, approp_null])
    assert errors == 2
