from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs13_2"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "legal_entity_country_code",
        "legal_entity_zip5",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Test LegalEntityZIP5 must be blank for foreign recipients (i.e., when LegalEntityCountryCode is not USA)
    USA doesn't affect success
    """
    fabs = FABSFactory(legal_entity_country_code="USA", legal_entity_zip5="12345", correction_delete_indicatr="")
    fabs_2 = FABSFactory(legal_entity_country_code="uSA", legal_entity_zip5="12345", correction_delete_indicatr=None)
    fabs_3 = FABSFactory(legal_entity_country_code="USA", legal_entity_zip5=None, correction_delete_indicatr="c")
    fabs_null = FABSFactory(legal_entity_country_code="UK", legal_entity_zip5=None, correction_delete_indicatr="C")
    fabs_null_2 = FABSFactory(legal_entity_country_code="UK", legal_entity_zip5="", correction_delete_indicatr="")
    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(legal_entity_country_code="UK", legal_entity_zip5="Test", correction_delete_indicatr="d")

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4, fabs_null, fabs_null_2])
    assert errors == 0


def test_failure(database):
    """Test failure when LegalEntityZIP5 isn't blank for foreign recipients"""

    fabs = FABSFactory(legal_entity_country_code="UK", legal_entity_zip5="Test", correction_delete_indicatr="")

    errors = number_of_errors(_FILE, database, models=[fabs])
    assert errors == 1
