from collections import OrderedDict

from dataactcore.utils import fileE_F
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import AwardProcurementFactory, AwardFinancialAssistanceFactory
from tests.unit.dataactcore.factories.domain import DunsFactory


def replicate_file_e_results(duns):
    """ Helper function for subaward results """
    return OrderedDict([
        ('AwardeeOrRecipientUniqueIdentifier', duns.awardee_or_recipient_uniqu),
        ('AwardeeOrRecipientLegalEntityName', duns.legal_business_name),
        ('UltimateParentUniqueIdentifier', duns.ultimate_parent_unique_ide),
        ('UltimateParentLegalEntityName', duns.ultimate_parent_legal_enti),
        ('HighCompOfficer1FullName', duns.high_comp_officer1_full_na),
        ('HighCompOfficer1Amount', duns.high_comp_officer1_amount),
        ('HighCompOfficer2FullName', duns.high_comp_officer2_full_na),
        ('HighCompOfficer2Amount', duns.high_comp_officer2_amount),
        ('HighCompOfficer3FullName', duns.high_comp_officer3_full_na),
        ('HighCompOfficer3Amount', duns.high_comp_officer3_amount),
        ('HighCompOfficer4FullName', duns.high_comp_officer4_full_na),
        ('HighCompOfficer4Amount', duns.high_comp_officer4_amount),
        ('HighCompOfficer5FullName', duns.high_comp_officer5_full_na),
        ('HighCompOfficer5Amount', duns.high_comp_officer5_amount)
    ])


def test_generate_file_e_sql(database, monkeypatch):
    """ test_generate_file_e_sql should provide the query representing E file data related to a submission """
    # Setup - create submission, awards, subawards
    sess = database.session

    sub1 = SubmissionFactory(submission_id=1)
    sub2 = SubmissionFactory(submission_id=2)

    d1_show = AwardProcurementFactory(submission_id=sub1.submission_id, awardee_or_recipient_uniqu='000000000')
    d2_show = AwardFinancialAssistanceFactory(submission_id=sub1.submission_id, awardee_or_recipient_uniqu='111111111')
    d1_hide = AwardProcurementFactory(submission_id=sub2.submission_id, awardee_or_recipient_uniqu='222222222')
    d2_hide = AwardFinancialAssistanceFactory(submission_id=sub2.submission_id, awardee_or_recipient_uniqu='333333333')

    duns_show = [DunsFactory(awardee_or_recipient_uniqu=(str(i)*9)) for i in range(0, 2)]
    duns_hide = [DunsFactory(awardee_or_recipient_uniqu=(str(i)*9)) for i in range(2, 4)]
    duns_s = duns_show + duns_hide

    sess.add_all([sub1, sub2, d1_hide, d1_show, d2_hide, d2_show] + duns_s)
    sess.commit()

    # Gather the sql
    file_e_query = fileE_F.generate_file_e_sql(sub1.submission_id)

    # Get the records
    file_e_records = sess.execute(file_e_query)
    file_e_cols = file_e_records.keys()
    file_e_value_sets = file_e_records.fetchall()
    file_e_results = [OrderedDict(list(zip(file_e_cols, file_e_value_set))) for file_e_value_set in file_e_value_sets]

    # Expected Results
    expected_file_e_results = [replicate_file_e_results(duns) for duns in duns_show]

    assert file_e_results == expected_file_e_results
