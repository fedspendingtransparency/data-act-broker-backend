from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from datetime import date
from dateutil.relativedelta import relativedelta

_FILE = "fabs4_3"


def test_column_headers(database):
    expected_subset = {"row_number", "action_date", "uniqueid_AssistanceTransactionUniqueKey"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Tests that future ActionDate is valid if it occurs within the current fiscal year."""
    tomorrow = date.today() + relativedelta(days=1)
    fabs_1 = FABSFactory(action_date=str(tomorrow), correction_delete_indicatr=None)
    fabs_2 = FABSFactory(action_date=None, correction_delete_indicatr="C")

    # Ignore non-dates
    fabs_3 = FABSFactory(action_date="5", correction_delete_indicatr="")

    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(action_date=str(tomorrow + relativedelta(years=1)), correction_delete_indicatr="D")

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    # If "tomorrow" is Oct 1 it's the end of the FY and we SHOULD see an error (there is no "later" in the FY to test
    # against)
    if tomorrow.day == 1 and tomorrow.month == 10:
        assert errors == 1
    else:
        assert errors == 0


def test_failure(database):
    """Tests that future ActionDate is invalid if it occurs outside the current fiscal year."""
    tomorrow = date.today() + relativedelta(years=1)
    fabs_1 = FABSFactory(action_date=str(tomorrow), correction_delete_indicatr="c")

    errors = number_of_errors(_FILE, database, models=[fabs_1])
    assert errors == 1
