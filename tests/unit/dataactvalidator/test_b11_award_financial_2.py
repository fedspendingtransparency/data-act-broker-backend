from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactcore.factories.domain import ObjectClassFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b11_award_financial_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'object_class', 'uniqueid_TAS', 'uniqueid_ObjectClass'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test valid object class code (3 digits) """

    af = AwardFinancialFactory(object_class='object_class')
    oc = ObjectClassFactory(object_class_code='object_class')

    assert number_of_errors(_FILE, database, models=[af, oc]) == 0


def test_failure(database):
    """ Test invalid object class code (3 digits) """

    # This should return because if it's '0000' '000', '00', '0' a warning should be returned
    af = AwardFinancialFactory(object_class='0000')
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(object_class='000')
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(object_class='00')
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(object_class='0')
    assert number_of_errors(_FILE, database, models=[af]) == 1
