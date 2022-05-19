from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs13_3'


def test_column_headers(database):
    expected_subset = {'row_number', 'legal_entity_country_code', 'record_type', 'legal_entity_zip5',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test LegalEntityZIP5 is required for domestic recipients (i.e., when LegalEntityCountryCode = USA)
        for non-aggregate and PII-redacted non-aggregate records (i.e., when RecordType = 2 or 3) record type 1 and
        non-USA don't affect success
    """
    fabs = FABSFactory(legal_entity_country_code='USA', record_type=2, legal_entity_zip5='12345',
                       correction_delete_indicatr=None)
    fabs_2 = FABSFactory(legal_entity_country_code='USA', record_type=3, legal_entity_zip5='12345',
                         correction_delete_indicatr='')
    fabs_3 = FABSFactory(legal_entity_country_code='UsA', record_type=3, legal_entity_zip5='12345',
                         correction_delete_indicatr='c')
    fabs_4 = FABSFactory(legal_entity_country_code='USA', record_type=1, legal_entity_zip5=None,
                         correction_delete_indicatr='C')
    fabs_5 = FABSFactory(legal_entity_country_code='UK', record_type=1, legal_entity_zip5='',
                         correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    fabs_6 = FABSFactory(legal_entity_country_code='UsA', record_type=2, legal_entity_zip5='',
                         correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6])
    assert errors == 0


def test_failure(database):
    """ Test failure when LegalEntityZIP5 is blank for domestic recipients for non-aggregate and PII-redacted
        non-aggregate records
    """

    fabs = FABSFactory(legal_entity_country_code='USA', record_type=2, legal_entity_zip5=None,
                       correction_delete_indicatr='')
    fabs_2 = FABSFactory(legal_entity_country_code='UsA', record_type=2, legal_entity_zip5='',
                         correction_delete_indicatr='C')
    fabs_3 = FABSFactory(legal_entity_country_code='USA', record_type=3, legal_entity_zip5=None,
                         correction_delete_indicatr='')
    fabs_4 = FABSFactory(legal_entity_country_code='USA', record_type=3, legal_entity_zip5='',
                         correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4])
    assert errors == 4
