from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs31_1"


def test_column_headers(database):
    expected_subset = {"row_number", "record_type", "business_types", "uei", "uniqueid_AssistanceTransactionUniqueKey"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """AwardeeOrRecipientUEI Field must be blank for aggregate and PII-redacted non-aggregate records
    (RecordType=1 or 3) and individual recipients (BusinessTypes includes 'P').
    """
    fabs_1 = FABSFactory(record_type=1, business_types="ABP", uei="", correction_delete_indicatr="")
    fabs_2 = FABSFactory(record_type=1, business_types="ABC", uei=None, correction_delete_indicatr=None)
    fabs_3 = FABSFactory(record_type=3, business_types="ABP", uei="", correction_delete_indicatr="c")
    fabs_4 = FABSFactory(record_type=3, business_types="ABC", uei="", correction_delete_indicatr="C")
    fabs_5 = FABSFactory(record_type=2, business_types="pbc", uei=None, correction_delete_indicatr="")
    fabs_6 = FABSFactory(record_type=2, business_types="PBC", uei="", correction_delete_indicatr="")
    # Ignore correction delete indicator of D
    fabs_7 = FABSFactory(record_type=2, business_types="ABP", uei="test", correction_delete_indicatr="d")

    # Doesn't fail if it's record type 2 and business_types don't contain P
    fabs_8 = FABSFactory(record_type=2, business_types="ABC", uei="test", correction_delete_indicatr="")

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7, fabs_8])
    assert errors == 0


def test_failure(database):
    """Test Failure for AwardeeOrRecipientUEI Fields must be blank for aggregate and PII-redacted non-aggregate records
    (RecordType=1 or 3) and individual recipients (BusinessTypes includes 'P').
    """

    fabs_1 = FABSFactory(record_type=1, business_types="ABC", uei="test", correction_delete_indicatr="")
    fabs_2 = FABSFactory(record_type=3, business_types="ABC", uei="test", correction_delete_indicatr="C")
    fabs_3 = FABSFactory(record_type=2, business_types="pbc", uei="test", correction_delete_indicatr=None)

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3])
    assert errors == 3
