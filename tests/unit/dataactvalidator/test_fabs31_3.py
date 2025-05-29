from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs31_3"


def test_column_headers(database):
    expected_subset = {"row_number", "uei", "uniqueid_AssistanceTransactionUniqueKey"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Test success for when AwardeeOrRecipientUEI is provided, it must be twelve characters."""
    fabs_1 = FABSFactory(uei="123456789aBc", correction_delete_indicatr="")
    fabs_2 = FABSFactory(uei="abc000000000", correction_delete_indicatr="C")
    fabs_3 = FABSFactory(uei="000000000000", correction_delete_indicatr=None)
    fabs_4 = FABSFactory(uei=None, correction_delete_indicatr=None)

    # Ignore correction delete indicator of D
    fabs_5 = FABSFactory(uei="2", correction_delete_indicatr="d")

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 0


def test_failure(database):
    """Test failure for when AwardeeOrRecipientUEI is provided, it must be twelve characters."""
    fabs_1 = FABSFactory(uei="2", correction_delete_indicatr="")
    fabs_2 = FABSFactory(uei="1234567s89aBc", correction_delete_indicatr="c")

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2])
    assert errors == 2
