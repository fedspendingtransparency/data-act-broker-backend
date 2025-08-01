from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = "b6_object_class_program_activity_1"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "prior_year_adjustment",
        "gross_outlays_undelivered_fyb",
        "ussgl480200_undelivered_or_fyb",
        "difference",
        "uniqueid_TAS",
        "uniqueid_DisasterEmergencyFundCode",
        "uniqueid_ProgramActivityCode",
        "uniqueid_ProgramActivityName",
        "uniqueid_ObjectClass",
        "uniqueid_ByDirectReimbursableFundingSource",
    }
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """Test Object Class Program Activity gross_outlays_undelivered_fyb equals ussgl480200_undelivered_or_fyb"""

    op = ObjectClassProgramActivityFactory(
        gross_outlays_undelivered_fyb=1, ussgl480200_undelivered_or_fyb=1, prior_year_adjustment="X"
    )
    # Different values, Different PYA
    op2 = ObjectClassProgramActivityFactory(
        gross_outlays_undelivered_fyb=0, ussgl480200_undelivered_or_fyb=1, prior_year_adjustment="A"
    )

    assert number_of_errors(_FILE, database, models=[op, op2]) == 0


def test_failure(database):
    """Test Object Class Program Activity gross_outlays_undelivered_fyb doesnt' equal
    ussgl480200_undelivered_or_fyb
    """

    op = ObjectClassProgramActivityFactory(
        gross_outlays_undelivered_fyb=1, ussgl480200_undelivered_or_fyb=0, prior_year_adjustment="x"
    )

    assert number_of_errors(_FILE, database, models=[op]) == 1
