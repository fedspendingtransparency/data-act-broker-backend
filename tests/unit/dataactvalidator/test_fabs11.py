from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs11'


def test_column_headers(database):
    expected_subset = {'row_number', 'record_type', 'legal_entity_address_line2',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test LegalEntityAddressLine2 is optional, but must be blank for aggregate and PII-redacted non-aggregate records
        (i.e., when RecordType = 1 or 3). Record type 2 doesn't affect success.
    """
    fabs = FABSFactory(record_type=2, legal_entity_address_line2='12345 Test Address', correction_delete_indicatr='')
    fabs_2 = FABSFactory(record_type=2, legal_entity_address_line2=None, correction_delete_indicatr=None)
    fabs_3 = FABSFactory(record_type=1, legal_entity_address_line2=None, correction_delete_indicatr='c')
    fabs_4 = FABSFactory(record_type=1, legal_entity_address_line2='', correction_delete_indicatr='C')
    fabs_5 = FABSFactory(record_type=3, legal_entity_address_line2=None, correction_delete_indicatr='')
    fabs_6 = FABSFactory(record_type=3, legal_entity_address_line2='', correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    fabs_7 = FABSFactory(record_type=3, legal_entity_address_line2='Test', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7])
    assert errors == 0


def test_failure(database):
    """ Test failure when LegalEntityAddressLine2 isn't blank for aggregate and PII-redacted non-aggregate records
        (i.e., when RecordType = 1 or 3).
    """

    fabs = FABSFactory(record_type=1, legal_entity_address_line2='Test', correction_delete_indicatr='')
    fabs_2 = FABSFactory(record_type=3, legal_entity_address_line2='Test', correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2])
    assert errors == 2
