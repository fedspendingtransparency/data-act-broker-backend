from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs9_1"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "record_type",
        "awardee_or_recipient_legal",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Test that awardee_or_recipient_legal contains "MULTIPLE RECIPIENTS" for aggregate records and other record
    types don't affect success.
    """
    fabs = FABSFactory(record_type=1, awardee_or_recipient_legal="MULTIPLE RECIPIENTS", correction_delete_indicatr="C")
    fabs_2 = FABSFactory(record_type=2, awardee_or_recipient_legal="TEST AGENCY", correction_delete_indicatr=None)
    fabs_3 = FABSFactory(record_type=3, awardee_or_recipient_legal="TEST AGENCY 2", correction_delete_indicatr="")

    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(record_type=1, awardee_or_recipient_legal="other", correction_delete_indicatr="d")

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """Test that awardee_or_recipient_legal without "MULTIPLE RECIPIENTS" for record type 1 fails."""

    fabs = FABSFactory(record_type=1, awardee_or_recipient_legal="MULTIPLE RECIPIENTS2", correction_delete_indicatr="")
    fabs_2 = FABSFactory(record_type=1, awardee_or_recipient_legal="other", correction_delete_indicatr="C")

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2])
    assert errors == 2
