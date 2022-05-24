from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs16'


def test_column_headers(database):
    expected_subset = {'row_number', 'legal_entity_country_code', 'legal_entity_foreign_provi', 'record_type',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ LegalEntityForeignProvinceName must be blank for domestic recipients (i.e., when LegalEntityCountryCode = USA)
        or RecordType = 1. Foreign reign recipients don't affect success.
    """

    fabs = FABSFactory(legal_entity_country_code='Japan', legal_entity_foreign_provi='Yamashiro', record_type=2,
                       correction_delete_indicatr='')
    fabs_2 = FABSFactory(legal_entity_country_code='UK', legal_entity_foreign_provi='', record_type=2,
                         correction_delete_indicatr=None)
    fabs_3 = FABSFactory(legal_entity_country_code='UK', legal_entity_foreign_provi=None, record_type=1,
                         correction_delete_indicatr='c')
    fabs_4 = FABSFactory(legal_entity_country_code='USA', legal_entity_foreign_provi=None, record_type=3,
                         correction_delete_indicatr='C')
    fabs_5 = FABSFactory(legal_entity_country_code='USA', legal_entity_foreign_provi='', record_type=2,
                         correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    fabs_6 = FABSFactory(legal_entity_country_code='UK', legal_entity_foreign_provi='Test', record_type=1,
                         correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6])
    assert errors == 0


def test_failure(database):
    """ Test failure LegalEntityForeignProvinceName must be blank for domestic recipients (i.e., when
        LegalEntityCountryCode = USA) or RecordType = 1
    """

    fabs = FABSFactory(legal_entity_country_code='UsA', legal_entity_foreign_provi='Test', record_type=2,
                       correction_delete_indicatr='')
    fabs_2 = FABSFactory(legal_entity_country_code='UK', legal_entity_foreign_provi='Test', record_type=1,
                         correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2])
    assert errors == 2
