from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from decimal import Decimal

_FILE = 'a24_appropriations'


def test_column_headers(database):
    expected_subset = {"row_number", "status_of_budgetary_resour_cpe", "budget_authority_available_cpe"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test that calculation works for equal values and for null """
    value = Decimal('100.23')
    approp = AppropriationFactory(status_of_budgetary_resour_cpe=value, budget_authority_available_cpe=value)
    approp_null = AppropriationFactory(status_of_budgetary_resour_cpe=0, budget_authority_available_cpe=None)

    errors = number_of_errors(_FILE, database, models=[approp, approp_null])
    assert errors == 0


def test_failure(database):
    """ Test that calculation fails for unequal values """
    approp = AppropriationFactory(status_of_budgetary_resour_cpe=Decimal(101.23),
                                  budget_authority_available_cpe=Decimal(102.34))

    errors = number_of_errors(_FILE, database, models=[approp])
    assert errors == 1
