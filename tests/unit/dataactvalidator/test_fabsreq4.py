from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabsreq4_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'business_funds_indicator', 'correction_delete_indicatr',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test BusinessFundsIndicator is required for all submissions except delete records. """

    fabs = FABSFactory(correction_delete_indicatr='C', business_funds_indicator='REC')
    fabs_2 = FABSFactory(correction_delete_indicatr='', business_funds_indicator='NON')

    # Test ignoring for D records
    fabs_3 = FABSFactory(correction_delete_indicatr='d', business_funds_indicator=None)
    fabs_4 = FABSFactory(correction_delete_indicatr='D', business_funds_indicator='')
    fabs_5 = FABSFactory(correction_delete_indicatr='D', business_funds_indicator='RE')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 0


def test_failure(database):
    """ Test fail BusinessFundsIndicator is required for all submissions except delete records. """

    fabs = FABSFactory(correction_delete_indicatr='c', business_funds_indicator=None)
    fabs_2 = FABSFactory(correction_delete_indicatr=None, business_funds_indicator='')

    errors = number_of_errors(_FILE, database, models=[fabs, fabs_2])
    assert errors == 2
