from tests.unit.dataactcore.factories.staging import FABSFactory
from dataactcore.models.domainModels import SAMRecipient, SAMRecipientUnregistered
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs31_7'


def test_column_headers(database):
    expected_subset = {'row_number', 'action_date', 'action_type', 'uei', 'federal_action_obligation',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_published_date_success(database):
    """ Test success for when ActionDate is after October 1, 2010 and ActionType = B, C, or D, AwardeeOrRecipientUEI
        should (when provided) have an active registration in SAM as of the ActionDate, except where
        FederalActionObligation is <=0 and ActionType = D, or, when ActionDate is after October 1, 2024,
        LegalEntityCountryCode is a foreign country.
    """
    recipient = SAMRecipient(uei='11111111111E', registration_date='01/01/2017', expiration_date='01/01/2018')
    exception_recipient = SAMRecipientUnregistered(uei='EEEEEEEEEEEE')
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
    # Special Unregistered Recipient Exception (action date, legal entity country, unregistered table)
    fabs_6 = FABSFactory(uei='EEEEEEEEEEEE', action_type='c', assistance_type='01', action_date='10/02/2024',
                         correction_delete_indicatr='', legal_entity_country_code='CAN', unique_award_key='active_key')

    errors = number_of_errors(_FILE, database, models=[recipient, exception_recipient, fabs_1, fabs_2, fabs_3, fabs_4,
                                                       fabs_5, fabs_6])
    assert errors == 0


def test_published_date_failure(database):
    """ Test failure for when ActionDate is after October 1, 2010 and ActionType = B, C, or D, AwardeeOrRecipientUEI
        should (when provided) have an active registration in SAM as of the ActionDate, except where
        FederalActionObligation is <=0 and ActionType = D, or, when ActionDate is after October 1, 2024,
        LegalEntityCountryCode is a foreign country.
    """
    recipient = SAMRecipient(uei='11111111111E', registration_date='01/01/2017', expiration_date='01/01/2018')
    exception_recipient = SAMRecipientUnregistered(uei='EEEEEEEEEEEE')
    fabs_1 = FABSFactory(uei='12345', action_type='b', action_date='06/20/2020', federal_action_obligation=10,
                         correction_delete_indicatr='')
    # FOA <= 0 and ActionType D checks individually
    fabs_2 = FABSFactory(uei='12345', action_type='d', action_date='06/20/2020', federal_action_obligation=1,
                         correction_delete_indicatr='')
    fabs_3 = FABSFactory(uei='12345', action_type='b', action_date='06/20/2020', federal_action_obligation=0,
                         correction_delete_indicatr='')
    # Ensuring that parts of the special exception on their own still fail. Only successful when combined.
    # older action date
    fabs_4 = FABSFactory(uei='EEEEEEEEEEEE', action_type='d', assistance_type='06', action_date='04/05/2022',
                         correction_delete_indicatr=None, legal_entity_country_code='CAN',
                         unique_award_key='inactive_key')
    # not foreign country code
    fabs_5 = FABSFactory(uei='EEEEEEEEEEEE', action_type='c', assistance_type='06', action_date='04/05/2024',
                         correction_delete_indicatr=None, legal_entity_country_code='USA',
                         unique_award_key='inactive_key')
    # not unregistered
    fabs_6 = FABSFactory(uei='UNKNOWNEEEEE', action_type='b', assistance_type='06', action_date='04/05/2024',
                         correction_delete_indicatr=None, legal_entity_country_code='CAN',
                         unique_award_key='inactive_key')
    errors = number_of_errors(_FILE, database, models=[recipient, exception_recipient, fabs_1, fabs_2, fabs_3, fabs_4,
                                                       fabs_5, fabs_6])
    assert errors == 6
