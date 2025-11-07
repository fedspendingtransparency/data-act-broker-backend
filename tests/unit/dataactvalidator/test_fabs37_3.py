from tests.unit.dataactcore.factories.staging import FABSFactory
from dataactcore.models.domainModels import AssistanceListing
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs37_3"


def test_column_headers(database):
    expected_subset = {"row_number", "assistance_listing_number", "uniqueid_AssistanceTransactionUniqueKey"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Test that no errors occur when the AssistanceListingNumber exists."""

    assistance_listing = AssistanceListing(program_number="1a.340")
    fabs_1 = FABSFactory(assistance_listing_number="1A.340", correction_delete_indicatr="")
    # Ignore correction delete indicator of D
    fabs_2 = FABSFactory(assistance_listing_number="AB.CDE", correction_delete_indicatr="d")

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, assistance_listing])
    assert errors == 0


def test_failure(database):
    """Test that its fails when AssistanceListingNumber does not exists."""

    # test for assistance_listing_number that doesn't exist in the table
    assistance_listing = AssistanceListing(program_number="12.340")
    fabs_1 = FABSFactory(assistance_listing_number="54.321", correction_delete_indicatr="")
    fabs_2 = FABSFactory(assistance_listing_number="AB.CDE", correction_delete_indicatr="c")
    fabs_3 = FABSFactory(assistance_listing_number="11.111", correction_delete_indicatr=None)

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, assistance_listing])
    assert errors == 3
