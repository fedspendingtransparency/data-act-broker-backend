from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs22_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'correction_delete_indicatr', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ When provided, CorrectionDeleteIndicator must contain one of the following values: C or D. """
    fabs_1 = FABSFactory(correction_delete_indicatr='')
    fabs_2 = FABSFactory(correction_delete_indicatr=None)
    fabs_3 = FABSFactory(correction_delete_indicatr='c')
    fabs_4 = FABSFactory(correction_delete_indicatr='D')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """ Test failure for when provided, CorrectionDeleteIndicator must contain one of the following values:
        C or D. """
    fabs_1 = FABSFactory(correction_delete_indicatr='A')
    fabs_2 = FABSFactory(correction_delete_indicatr='Z')
    fabs_3 = FABSFactory(correction_delete_indicatr='cd')
    fabs_4 = FABSFactory(correction_delete_indicatr='L')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 4
