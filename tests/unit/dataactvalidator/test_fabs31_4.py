from tests.unit.dataactcore.factories.staging import FABSFactory, PublishedFABSFactory
from dataactcore.models.domainModels import SAMRecipient, SAMRecipientUnregistered
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs31_4'


def test_column_headers(database):
    expected_subset = {'row_number', 'assistance_type', 'action_date', 'uei', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_published_date_success(database):
    """ Test success for when provided, AwardeeOrRecipientUEI must be registered (not necessarily active) in SAM,
        unless the ActionDate is before October 1, 2010, or, when ActionDate is after October 1, 2024,
        LegalEntityCountryCode is a foreign country.
    """

    pub_fabs_1 = PublishedFABSFactory(unique_award_key='active_key', action_date='20091001', is_active=True)
    pub_fabs_2 = PublishedFABSFactory(unique_award_key='inactive_key', action_date='20091001', is_active=False)
    models = [pub_fabs_1, pub_fabs_2]

    # new records that may or may not be related to older awards
    recipient = SAMRecipient(uei='22222222222E')
    exception_recipient = SAMRecipientUnregistered(uei='EEEEEEEEEEEE')
    fabs_1 = FABSFactory(uei=recipient.uei, assistance_type='02', action_date='10/02/2010',
                         correction_delete_indicatr='', unique_award_key='active_key')
    fabs_2 = FABSFactory(uei=recipient.uei.lower(), assistance_type='02', action_date='10/02/2010',
                         correction_delete_indicatr='c', unique_award_key='active_key')
    fabs_3 = FABSFactory(uei=None, assistance_type='01', action_date='10/02/2010', correction_delete_indicatr='c',
                         unique_award_key='active_key')
    # Before October 1, 2010
    fabs_4 = FABSFactory(uei='12345', assistance_type='02', action_date='09/30/2010', correction_delete_indicatr='C',
                         unique_award_key='new_key')
    # Ignore correction delete indicator of D
    fabs_5 = FABSFactory(uei='12345', assistance_type='01', action_date='10/02/2010', correction_delete_indicatr='d',
                         unique_award_key='inactive_key')
    # Special Unregistered Recipient Exception (action date, legal entity country, unregistered table)
    fabs_6 = FABSFactory(uei='EEEEEEEEEEEE', assistance_type='01', action_date='10/02/2024',
                         correction_delete_indicatr='', legal_entity_country_code='CAN', unique_award_key='active_key')

    models += [recipient, exception_recipient, fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6]

    errors = number_of_errors(_FILE, database, models=models)
    assert errors == 0


def test_published_date_failure(database):
    """ Test failure for when provided, AwardeeOrRecipientUEI must be registered (not necessarily active) in SAM,
        unless the ActionDate is before October 1, 2010, or, when ActionDate is after October 1, 2024,
        LegalEntityCountryCode is a foreign country.
    """

    pub_fabs_1 = PublishedFABSFactory(unique_award_key='active_key', action_date='20220404', is_active=True)
    pub_fabs_2 = PublishedFABSFactory(unique_award_key='inactive_key', action_date='20091001', is_active=False)
    models = [pub_fabs_1, pub_fabs_2]

    # new records that may or may not be related to older awards
    recipient = SAMRecipient(uei='1111111111111E')
    exception_recipient = SAMRecipientUnregistered(uei='EEEEEEEEEEEE')
    fabs_1 = FABSFactory(uei='12345', assistance_type='02', action_date='10/02/2010', correction_delete_indicatr='',
                         unique_award_key='active_key')
    fabs_2 = FABSFactory(uei='12345', assistance_type='06', action_date='04/05/2022', correction_delete_indicatr=None,
                         unique_award_key='inactive_key')
    # Ensuring that parts of the special exception on their own still fail. Only successful when combined.
    # older action date
    fabs_3 = FABSFactory(uei='EEEEEEEEEEEE', assistance_type='06', action_date='04/05/2022',
                         correction_delete_indicatr=None, legal_entity_country_code='CAN',
                         unique_award_key='inactive_key')
    # not foreign country code
    fabs_4 = FABSFactory(uei='EEEEEEEEEEEE', assistance_type='06', action_date='04/05/2024',
                         correction_delete_indicatr=None, legal_entity_country_code='USA',
                         unique_award_key='inactive_key')
    # not unregistered
    fabs_5 = FABSFactory(uei='UNKNOWNEEEEE', assistance_type='06', action_date='04/05/2024',
                         correction_delete_indicatr=None, legal_entity_country_code='CAN',
                         unique_award_key='inactive_key')
    models += [recipient, exception_recipient, fabs_1, fabs_2, fabs_3, fabs_4, fabs_5]

    errors = number_of_errors(_FILE, database, models=models)
    assert errors == 5
