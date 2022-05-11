from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs36'


def test_column_headers(database):
    expected_subset = {'row_number', 'cfda_number', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test valid. CFDA_Number must be in XX.XXX format """

    fabs_1 = FABSFactory(cfda_number='99.999', correction_delete_indicatr='')
    fabs_2 = FABSFactory(cfda_number='12.345', correction_delete_indicatr='c')
    # Ignore correction delete indicator of D
    fabs_3 = FABSFactory(cfda_number='1234', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3])
    assert errors == 0


def test_failure(database):
    """ Test invalid. CFDA_Number must be in XX.XXX format """

    fabs_1 = FABSFactory(cfda_number='1234', correction_delete_indicatr='')
    fabs_2 = FABSFactory(cfda_number='12.34567', correction_delete_indicatr=None)
    fabs_3 = FABSFactory(cfda_number='12.3', correction_delete_indicatr='c')
    fabs_4 = FABSFactory(cfda_number='123.456', correction_delete_indicatr='C')
    fabs_5 = FABSFactory(cfda_number='ab.cdf', correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 5
