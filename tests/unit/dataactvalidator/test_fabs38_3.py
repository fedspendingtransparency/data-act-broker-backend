from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs38_3'


def test_column_headers(database):
    expected_subset = {'row_number', 'awarding_office_code', 'action_type', 'correction_delete_indicatr', 'action_date',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test AwardingOfficeCode must be submitted for new awards (ActionType = A) or mixed aggregate records
        (ActionType = E) whose ActionDate is on or after October 1, 2018, and whose CorrectionDeleteIndicator is either
        Blank or C.
    """

    # All factors as stated
    fabs_1 = FABSFactory(awarding_office_code='AAAAAA', action_type='A', action_date='10/01/2018',
                         correction_delete_indicatr='')
    fabs_2 = FABSFactory(awarding_office_code='111111', action_type='e', action_date='10/01/2018',
                         correction_delete_indicatr='C')

    # Rule ignored for earlier dates
    fabs_3 = FABSFactory(awarding_office_code='', action_type='E', action_date='10/01/2017',
                         correction_delete_indicatr='C')

    # Rule ignored for other action types
    fabs_4 = FABSFactory(awarding_office_code=None, action_type='B', action_date='10/01/2018',
                         correction_delete_indicatr='')

    # Rule ignored for CorrectionDeleteIndicator of D
    fabs_5 = FABSFactory(awarding_office_code=None, action_type='A', action_date='10/01/2018',
                         correction_delete_indicatr='D')

    # Rule ignored for action type is None
    fabs_6 = FABSFactory(awarding_office_code='', action_type=None, action_date='10/01/2018',
                         correction_delete_indicatr='')
    fabs_7 = FABSFactory(awarding_office_code=None, action_type=None, action_date='10/02/2018',
                         correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7])
    assert errors == 0


def test_failure(database):
    """ Test failure AwardingOfficeCode must be submitted for new awards (ActionType = A) or mixed aggregate records
        (ActionType = E) whose ActionDate is on or after October 1, 2018, and whose CorrectionDeleteIndicator is either
        Blank or C.
    """

    fabs_1 = FABSFactory(awarding_office_code='', action_type='A', action_date='10/01/2018',
                         correction_delete_indicatr='')
    fabs_2 = FABSFactory(awarding_office_code=None, action_type='e', action_date='10/02/2018',
                         correction_delete_indicatr='C')
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2])
    assert errors == 2
