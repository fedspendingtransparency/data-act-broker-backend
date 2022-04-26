from collections import OrderedDict

from dataactcore.utils import fileE_F
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import AwardProcurementFactory, AwardFinancialAssistanceFactory
from tests.unit.dataactcore.factories.domain import SAMRecipientFactory


def replicate_file_e_results(recipient):
    """ Helper function for subaward results """
    return OrderedDict([
        ('AwardeeOrRecipientUEI', recipient.uei),
        ('AwardeeOrRecipientLegalEntityName', recipient.legal_business_name),
        ('UltimateParentUEI', recipient.ultimate_parent_uei),
        ('UltimateParentLegalEntityName', recipient.ultimate_parent_legal_enti),
        ('HighCompOfficer1FullName', recipient.high_comp_officer1_full_na),
        ('HighCompOfficer1Amount', recipient.high_comp_officer1_amount),
        ('HighCompOfficer2FullName', recipient.high_comp_officer2_full_na),
        ('HighCompOfficer2Amount', recipient.high_comp_officer2_amount),
        ('HighCompOfficer3FullName', recipient.high_comp_officer3_full_na),
        ('HighCompOfficer3Amount', recipient.high_comp_officer3_amount),
        ('HighCompOfficer4FullName', recipient.high_comp_officer4_full_na),
        ('HighCompOfficer4Amount', recipient.high_comp_officer4_amount),
        ('HighCompOfficer5FullName', recipient.high_comp_officer5_full_na),
        ('HighCompOfficer5Amount', recipient.high_comp_officer5_amount)
    ])


def test_generate_file_e_sql(database, monkeypatch):
    """ test_generate_file_e_sql should provide the query representing E file data related to a submission """
    # Setup - create submission, awards, subawards
    sess = database.session

    sub1 = SubmissionFactory(submission_id=1)
    sub2 = SubmissionFactory(submission_id=2)

    d1_show = AwardProcurementFactory(submission_id=sub1.submission_id, awardee_or_recipient_uei='00000000000e')
    d2_show = AwardFinancialAssistanceFactory(submission_id=sub1.submission_id, awardee_or_recipient_uei='11111111111e')
    d1_hide = AwardProcurementFactory(submission_id=sub2.submission_id, awardee_or_recipient_uei='22222222222e')
    d2_hide = AwardFinancialAssistanceFactory(submission_id=sub2.submission_id, awardee_or_recipient_uei='33333333333e')

    recipient_show = [SAMRecipientFactory(uei=(str(i) * 11) + 'e') for i in range(0, 2)]
    recipient_hide = [SAMRecipientFactory(uei=(str(i) * 11) + 'e') for i in range(2, 4)]
    recipient_s = recipient_show + recipient_hide

    sess.add_all([sub1, sub2, d1_hide, d1_show, d2_hide, d2_show] + recipient_s)
    sess.commit()

    # Gather the sql
    file_e_query = fileE_F.generate_file_e_sql(sub1.submission_id)

    # Get the records
    file_e_records = sess.execute(file_e_query)
    file_e_cols = file_e_records.keys()
    file_e_value_sets = file_e_records.fetchall()
    file_e_results = [OrderedDict(list(zip(file_e_cols, file_e_value_set))) for file_e_value_set in file_e_value_sets]

    # Expected Results
    expected_file_e_results = [replicate_file_e_results(recipient) for recipient in recipient_show]

    assert file_e_results == expected_file_e_results
