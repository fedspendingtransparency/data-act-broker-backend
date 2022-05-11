from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactcore.factories.domain import ZipsFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs35_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'legal_entity_zip5', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ LegalEntityZIP5 is not a valid zip code. Null/blank zip codes ignored. """
    zip_1 = ZipsFactory(zip5='12345')
    fabs_1 = FABSFactory(legal_entity_zip5='12345', correction_delete_indicatr='')
    fabs_2 = FABSFactory(legal_entity_zip5=None, correction_delete_indicatr=None)
    fabs_3 = FABSFactory(legal_entity_zip5='', correction_delete_indicatr='c')
    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(legal_entity_zip5='54321', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[zip_1, fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """ LegalEntityZIP5 is not a valid zip code. """
    zip_1 = ZipsFactory(zip5='12345')
    fabs_1 = FABSFactory(legal_entity_zip5='54321', correction_delete_indicatr='')
    # add a valid one to make sure NOT EXISTS is doing what we expect
    fabs_2 = FABSFactory(legal_entity_zip5='12345', correction_delete_indicatr='c')

    errors = number_of_errors(_FILE, database, models=[zip_1, fabs_1, fabs_2])
    assert errors == 1
