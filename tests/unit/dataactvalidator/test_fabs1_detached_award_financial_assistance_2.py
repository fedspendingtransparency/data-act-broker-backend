from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs1_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'record_type', 'fain', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test that record_type 2 + 3 doesn't affect success (can have a FAIN) and that FAIN check works. """
    fabs = FABSFactory(record_type=1, fain='', correction_delete_indicatr='')
    fabs_2 = FABSFactory(record_type=2, fain='17TCEP0034', correction_delete_indicatr='C')
    fabs_3 = FABSFactory(record_type=3, fain='17TCEP0034', correction_delete_indicatr=None)
    fabs_null = FABSFactory(record_type=1, fain=None, correction_delete_indicatr='c')
    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(record_type=1, fain='abc', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4, fabs_null])
    assert errors == 0


def test_failure(database):
    """ Test that a non-null FAIN with record type 1 returns an error. """

    fabs = FABSFactory(record_type=1, fain='abc', correction_delete_indicatr=None)

    errors = number_of_errors(_FILE, database, models=[fabs])
    assert errors == 1
