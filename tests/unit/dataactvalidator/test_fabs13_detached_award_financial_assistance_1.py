from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs13_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'record_type', 'legal_entity_zip5', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test LegalEntityZIP5 must be blank for aggregate records (i.e., when RecordType = 1) record types 2 and 3 don't
        affect success.
    """
    fabs = FABSFactory(record_type=2, legal_entity_zip5='12345', correction_delete_indicatr='')
    fabs_2 = FABSFactory(record_type=2, legal_entity_zip5=None, correction_delete_indicatr=None)
    fabs_3 = FABSFactory(record_type=3, legal_entity_zip5='12345', correction_delete_indicatr=None)
    fabs_null = FABSFactory(record_type=1, legal_entity_zip5=None, correction_delete_indicatr='c')
    fabs_null_2 = FABSFactory(record_type=1, legal_entity_zip5='', correction_delete_indicatr='C')
    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(record_type=1, legal_entity_zip5='Test', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_null, fabs_null_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """ Test failure when LegalEntityZIP5 isn't blank for aggregate records (i.e., when RecordType = 1) """

    fabs = FABSFactory(record_type=1, legal_entity_zip5='Test', correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[fabs])
    assert errors == 1
