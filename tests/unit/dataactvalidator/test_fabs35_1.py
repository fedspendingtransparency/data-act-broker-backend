from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs35_1"


def test_column_headers(database):
    expected_subset = {"row_number", "legal_entity_zip_last4", "uniqueid_AssistanceTransactionUniqueKey"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """When provided, LegalEntityZIPLast4 must be in the format ####."""
    fabs_1 = FABSFactory(legal_entity_zip_last4="1234", correction_delete_indicatr="")
    fabs_2 = FABSFactory(legal_entity_zip_last4=None, correction_delete_indicatr="c")
    fabs_3 = FABSFactory(legal_entity_zip_last4="", correction_delete_indicatr=None)
    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(legal_entity_zip_last4="12345", correction_delete_indicatr="d")

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """When provided, LegalEntityZIPLast4 must be in the format ####."""
    fabs_1 = FABSFactory(legal_entity_zip_last4="123", correction_delete_indicatr="")
    fabs_2 = FABSFactory(legal_entity_zip_last4="12345", correction_delete_indicatr=None)
    fabs_3 = FABSFactory(legal_entity_zip_last4="ABCD", correction_delete_indicatr="c")
    fabs_4 = FABSFactory(legal_entity_zip_last4="123D", correction_delete_indicatr="C")

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 4
