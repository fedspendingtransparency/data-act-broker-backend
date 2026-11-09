from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactcore.factories.domain import ObjectClassFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = "b11_award_financial_1"


def test_column_headers(database):
    expected_subset = {"row_number", "object_class", "uniqueid_TAS", "uniqueid_ObjectClass"}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """Test valid object class code (3 digits)"""

    af = AwardFinancialFactory(object_class="object_class")
    oc = ObjectClassFactory(object_class_code="object_class")

    assert number_of_errors(_FILE, database, models=[af, oc]) == 0


def test_failure(database):
    """Test invalid object class code (3 digits)"""

    af1 = AwardFinancialFactory(object_class="object_class")
    oc1 = ObjectClassFactory(object_class_code="object_class_bad")

    # Should fail even if 920/9200 are loaded codes
    af2 = AwardFinancialFactory(object_class="920")
    oc2 = ObjectClassFactory(object_class_code="920")

    af3 = AwardFinancialFactory(object_class="9200")
    oc3 = ObjectClassFactory(object_class_code="9200")

    assert number_of_errors(_FILE, database, models=[af1, oc1, af2, oc2, af3, oc3]) == 3
