from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a31_award_financial'
_TAS = 'a31_award_financial_tas'


def test_column_headers(database):
    expected_subset = {'row_number', 'availability_type_code',
                       'beginning_period_of_availa', 'ending_period_of_availabil'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that Beginning Period of Availability and Ending Period of Availability are blank
    if Availability Type Code = X """
    tas = "".join([_TAS, "_success"])

    af1 = AwardFinancialFactory(availability_type_code='x', beginning_period_of_availa=None,
                               ending_period_of_availabil=None)
    af2 = AwardFinancialFactory(availability_type_code='X', beginning_period_of_availa=None,
                               ending_period_of_availabil=None)

    assert number_of_errors(_FILE, database, models=[af1, af2]) == 0


def test_failure(database):
    """ Tests that Beginning Period of Availability and Ending Period of Availability are not blank
    if Availability Type Code = X """
    tas = "".join([_TAS, "_failure"])

    af1 = AwardFinancialFactory(availability_type_code='x', beginning_period_of_availa='Today',
                               ending_period_of_availabil='Today')
    af2 = AwardFinancialFactory(availability_type_code='x', beginning_period_of_availa='Today',
                               ending_period_of_availabil=None)
    af3 = AwardFinancialFactory(availability_type_code='x', beginning_period_of_availa=None,
                               ending_period_of_availabil='Today')
    af4 = AwardFinancialFactory(availability_type_code='X', beginning_period_of_availa='Today',
                               ending_period_of_availabil='Today')
    af5 = AwardFinancialFactory(availability_type_code='X', beginning_period_of_availa='Today',
                               ending_period_of_availabil=None)
    af6 = AwardFinancialFactory(availability_type_code='X', beginning_period_of_availa=None,
                               ending_period_of_availabil='Today')

    assert number_of_errors(_FILE, database, models=[af1, af2, af3, af4, af5, af6]) == 6
