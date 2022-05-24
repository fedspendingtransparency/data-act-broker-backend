from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs30'


def test_column_headers(database):
    expected_subset = {'row_number', 'business_funds_indicator', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ BusinessFundsIndicator must contain one of the following values: REC or NON. Case doesn't matter """

    fabs = FABSFactory(business_funds_indicator='REC', correction_delete_indicatr='c')
    fabs_2 = FABSFactory(business_funds_indicator='non', correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    fabs_3 = FABSFactory(business_funds_indicator='red', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3])
    assert errors == 0


def test_failure(database):
    """ BusinessFundsIndicator must contain one of the following values: REC or NON. """

    fabs = FABSFactory(business_funds_indicator='RECs', correction_delete_indicatr='C')
    fabs_2 = FABSFactory(business_funds_indicator='red', correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2])
    assert errors == 2
