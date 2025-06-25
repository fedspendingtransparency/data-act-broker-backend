from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabsreq9"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "awardee_or_recipient_legal",
        "correction_delete_indicatr",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Test AwardeeOrRecipientLegalEntityName is required for all submissions except delete records."""

    fabs = FABSFactory(correction_delete_indicatr="C", awardee_or_recipient_legal="REDACTED")
    fabs_2 = FABSFactory(correction_delete_indicatr="", awardee_or_recipient_legal="Name")

    # Test ignoring for D records
    fabs_3 = FABSFactory(correction_delete_indicatr="d", awardee_or_recipient_legal=None)
    fabs_4 = FABSFactory(correction_delete_indicatr="D", awardee_or_recipient_legal="")
    fabs_5 = FABSFactory(correction_delete_indicatr="d", awardee_or_recipient_legal="Name")

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 0


def test_failure(database):
    """Test fail AwardeeOrRecipientLegalEntityName is required for all submissions except delete records."""

    fabs = FABSFactory(correction_delete_indicatr="c", awardee_or_recipient_legal=None)
    fabs_2 = FABSFactory(correction_delete_indicatr=None, awardee_or_recipient_legal="")

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2])
    assert errors == 2
