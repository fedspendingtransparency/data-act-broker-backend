from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactcore.factories.domain import ObjectClassFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = "b11_object_class_program_activity_1"


def test_column_headers(database):
    expected_subset = {"row_number", "object_class", "uniqueid_TAS", "uniqueid_ObjectClass"}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """Test valid object class code (3 digits)"""

    op = ObjectClassProgramActivityFactory(object_class="object_class")
    oc = ObjectClassFactory(object_class_code="object_class")

    assert number_of_errors(_FILE, database, models=[op, oc]) == 0


def test_failure(database):
    """Test invalid object class code (3 digits)"""

    op = ObjectClassProgramActivityFactory(object_class="object_class")
    oc = ObjectClassFactory(object_class_code="object_class_bad")

    assert number_of_errors(_FILE, database, models=[op, oc]) == 1
