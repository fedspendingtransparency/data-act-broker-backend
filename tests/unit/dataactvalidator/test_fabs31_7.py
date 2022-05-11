from tests.unit.dataactcore.factories.staging import FABSFactory
from dataactcore.models.domainModels import SAMRecipient
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs31_7'


def test_column_headers(database):
    expected_subset = {'row_number', 'action_date', 'action_type', 'uei', 'federal_action_obligation',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_pubished_date_success(database):
    """ Test success for when ActionDate is after October 1, 2010 and ActionType = B, C, or D, AwardeeOrRecipientUEI
        should (when provided) have an active registration in SAM as of the ActionDate, except where
        FederalActionObligation is <=0 and ActionType = D.
    """
    recipient = SAMRecipient(uei='11111111111E', registration_date='01/01/2017', expiration_date='01/01/2018')
    fabs_1 = FABSFactory(uei=recipient.uei, action_type='b', action_date='06/22/2017', federal_action_obligation=10,
                         correction_delete_indicatr='')
    # Ignore different action type
    fabs_2 = FABSFactory(uei='12345', action_type='a', action_date='06/20/2017', federal_action_obligation=10,
                         correction_delete_indicatr='')
    # Ignore FOA <= 0 and ActionType D
    fabs_3 = FABSFactory(uei='12345', action_type='d', action_date='06/20/2017', federal_action_obligation=-10,
                         correction_delete_indicatr='')
    # Ignore Before October 1, 2010
    fabs_4 = FABSFactory(uei='12345', action_type='B', action_date='09/30/2010', federal_action_obligation=10,
                         correction_delete_indicatr=None)
    # Ignore correction delete indicator of D
    fabs_5 = FABSFactory(uei='12345', action_type='c', action_date='06/20/2020', federal_action_obligation=10,
                         correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[recipient, fabs_1, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 0


def test_pubished_date_failure(database):
    """ Test failure for when ActionDate is after October 1, 2010 and ActionType = B, C, or D, AwardeeOrRecipientUEI
        should (when provided) have an active registration in SAM as of the ActionDate, except where
        FederalActionObligation is <=0 and ActionType = D.
    """
    recipient = SAMRecipient(uei='11111111111E', registration_date='01/01/2017', expiration_date='01/01/2018')
    fabs_1 = FABSFactory(uei='12345', action_type='b', action_date='06/20/2020', federal_action_obligation=10,
                         correction_delete_indicatr='')
    # FOA <= 0 and ActionType D checks individually
    fabs_2 = FABSFactory(uei='12345', action_type='d', action_date='06/20/2020', federal_action_obligation=1,
                         correction_delete_indicatr='')
    fabs_3 = FABSFactory(uei='12345', action_type='b', action_date='06/20/2020', federal_action_obligation=0,
                         correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[recipient, fabs_1, fabs_2, fabs_3])
    assert errors == 3
