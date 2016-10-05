from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'a4_appropriations'

def test_column_headers(database):
    expected_subset = {"row_number", "status_of_budgetary_resour_cpe", "obligations_incurred_total_cpe",
        "unobligated_balance_cpe"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual

def test_success(database):
    """ Test that TAS values can be found, and null matches work correctly"""
    approp = AppropriationFactory(status_of_budgetary_resour_cpe = 300, obligations_incurred_total_cpe = 100,
                                  unobligated_balance_cpe = 200)
    approp_null = AppropriationFactory(status_of_budgetary_resour_cpe = 100, obligations_incurred_total_cpe = 100,
                                       unobligated_balance_cpe = None)

    errors = number_of_errors(_FILE, database, models=[approp, approp_null])
    assert errors == 0

def test_failure(database):
    """ Test that tas that does not match is an error"""

    approp = AppropriationFactory(status_of_budgetary_resour_cpe = 500, obligations_incurred_total_cpe = 100,
                                  unobligated_balance_cpe = 200)
    approp_null = AppropriationFactory(status_of_budgetary_resour_cpe = 300, obligations_incurred_total_cpe = 100,
                                       unobligated_balance_cpe = None)

    errors = number_of_errors(_FILE, database, models=[approp, approp_null])
    assert errors == 2