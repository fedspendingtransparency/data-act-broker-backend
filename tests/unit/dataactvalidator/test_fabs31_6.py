from tests.unit.dataactcore.factories.staging import FABSFactory
from dataactcore.models.domainModels import SAMRecipient, SAMRecipientUnregistered
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs31_6'


def test_column_headers(database):
    expected_subset = {'row_number', 'action_date', 'action_type', 'uei', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_published_date_success(database):
    """ Test success for when ActionDate is after October 1, 2010 and ActionType = A, AwardeeOrRecipientUEI should
        (when provided) have an active registration in SAM as of the ActionDate, unless, when ActionDate is after
        October 1, 2024, LegalEntityCountryCode is a foreign country. This is a warning because
        CorrectionDeleteIndicator is C and the action date is before January 1, 2017.
    """
    recipient = SAMRecipient(uei='11111111111E', registration_date='01/01/2016', expiration_date='01/01/2017')
    exception_recipient = SAMRecipientUnregistered(uei='EEEEEEEEEEEE')
    fabs_1 = FABSFactory(uei=recipient.uei, action_type='a', action_date='06/22/2016', correction_delete_indicatr='C')
    # Ignore different action type
    fabs_2 = FABSFactory(uei='12345', action_type='B', action_date='06/20/2015', correction_delete_indicatr='C')
    # Ignore Before October 1, 2010
    fabs_3 = FABSFactory(uei='12345', action_type='a', action_date='09/30/2010', correction_delete_indicatr='C')
    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(uei='12345', action_type='A', action_date='06/20/2015', correction_delete_indicatr='D')
    # Special Unregistered Recipient Exception (action date, legal entity country, unregistered table)
    fabs_5 = FABSFactory(uei='EEEEEEEEEEEE', action_type='A', assistance_type='01', action_date='10/02/2024',
                         correction_delete_indicatr='C', legal_entity_country_code='CAN', unique_award_key='active_key')
    # These would emit warnings but since it's caught by FABS 31.5, FABS 31.6 ignores it.
    fabs_6 = FABSFactory(uei='11111111111E', action_type='A', action_date='06/20/2019', correction_delete_indicatr='c')
    fabs_7 = FABSFactory(uei='11111111111E', action_type='A', action_date='06/20/2015', correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[recipient, exception_recipient, fabs_1, fabs_2, fabs_3, fabs_4,
                                                       fabs_5, fabs_6, fabs_7])
    assert errors == 0


def test_published_date_failure(database):
    """ Test failure for when ActionDate is after October 1, 2010 and ActionType = A, AwardeeOrRecipientUEI should
        (when provided) have an active registration in SAM as of the ActionDate, unless, when ActionDate is after
        October 1, 2024, LegalEntityCountryCode is a foreign country. This is a warning because
        CorrectionDeleteIndicator is C and the action date is before January 1, 2017.
    """
    recipient = SAMRecipient(uei='111111111111', registration_date='01/01/2017', expiration_date='01/01/2018')
    exception_recipient = SAMRecipientUnregistered(uei='EEEEEEEEEEEE')
    fabs_1 = FABSFactory(uei='12345', action_type='A', action_date='06/20/2016', correction_delete_indicatr='c')
    # Ensuring that parts of the special exception on their own still fail. Only successful when combined.
    # older action date
    fabs_2 = FABSFactory(uei='EEEEEEEEEEEE', action_type='A', assistance_type='06', action_date='04/05/2016',
                         correction_delete_indicatr='C', legal_entity_country_code='CAN',
                         unique_award_key='inactive_key')
    # not foreign country code
    fabs_3 = FABSFactory(uei='EEEEEEEEEEEE', action_type='A', assistance_type='06', action_date='04/05/2016',
                         correction_delete_indicatr='c', legal_entity_country_code='USA',
                         unique_award_key='inactive_key')
    # not unregistered
    fabs_4 = FABSFactory(uei='UNKNOWNEEEEE', action_type='A', assistance_type='06', action_date='04/05/2016',
                         correction_delete_indicatr='C', legal_entity_country_code='CAN',
                         unique_award_key='inactive_key')
    errors = number_of_errors(_FILE, database, models=[recipient, exception_recipient, fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 4
