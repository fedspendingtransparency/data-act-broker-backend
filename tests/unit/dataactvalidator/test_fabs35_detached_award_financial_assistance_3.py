from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactcore.factories.domain import ZipsFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs35_detached_award_financial_assistance_3'


def test_column_headers(database):
    expected_subset = {'row_number', 'legal_entity_zip5', 'legal_entity_zip_last4',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ LegalEntityZIP5 + LegalEntityZIPLast4 is not a valid 9 digit zip. Null/blank zip codes ignored. """
    zip_1 = ZipsFactory(zip5='12345', zip_last4='6789')
    fabs_1 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4='6789', correction_delete_indicatr='')
    fabs_2 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4=None, correction_delete_indicatr=None)
    fabs_3 = FABSFactory(legal_entity_zip5=None, legal_entity_zip_last4='6789', correction_delete_indicatr='c')
    fabs_4 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4='', correction_delete_indicatr='C')
    fabs_5 = FABSFactory(legal_entity_zip5='', legal_entity_zip_last4='6789', correction_delete_indicatr='')

    # half bad code but other half blank ignored
    fabs_6 = FABSFactory(legal_entity_zip5='', legal_entity_zip_last4='9876', correction_delete_indicatr='')
    fabs_7 = FABSFactory(legal_entity_zip5='54321', legal_entity_zip_last4='', correction_delete_indicatr='')

    # Ignore correction delete indicator of D
    fabs_8 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4='9876', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[zip_1, fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7,
                                                       fabs_8])
    assert errors == 0


def test_failure(database):
    """ LegalEntityZIP5 is not a valid zip code. """
    zip_1 = ZipsFactory(zip5='12345', zip_last4='6789')
    fabs_1 = FABSFactory(legal_entity_zip5='12345', legal_entity_zip_last4='9876', correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[zip_1, fabs_1])
    assert errors == 1
