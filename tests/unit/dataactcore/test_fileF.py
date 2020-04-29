from collections import OrderedDict

from dataactcore.utils import fileE_F
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import AwardProcurementFactory, AwardFinancialAssistanceFactory
from tests.unit.dataactcore.factories.fsrs import SubawardFactory


def replicate_file_f_results(subaward):
    """ Helper function for subaward results """
    return OrderedDict([
        ('PrimeAwardUniqueKey', subaward.unique_award_key),
        ('PrimeAwardID', subaward.award_id),
        ('ParentAwardID', subaward.parent_award_id),
        ('PrimeAwardAmount', subaward.award_amount),
        ('ActionDate', subaward.action_date),
        ('PrimeAwardFiscalYear', subaward.fy),
        ('AwardingAgencyCode', subaward.awarding_agency_code),
        ('AwardingAgencyName', subaward.awarding_agency_name),
        ('AwardingSubTierAgencyCode', subaward.awarding_sub_tier_agency_c),
        ('AwardingSubTierAgencyName', subaward.awarding_sub_tier_agency_n),
        ('AwardingOfficeCode', subaward.awarding_office_code),
        ('AwardingOfficeName', subaward.awarding_office_name),
        ('FundingAgencyCode', subaward.funding_agency_code),
        ('FundingAgencyName', subaward.funding_agency_name),
        ('FundingSubTierAgencyCode', subaward.funding_sub_tier_agency_co),
        ('FundingSubTierAgencyName', subaward.funding_sub_tier_agency_na),
        ('FundingOfficeCode', subaward.funding_office_code),
        ('FundingOfficeName', subaward.funding_office_name),
        ('AwardeeOrRecipientUniqueIdentifier', subaward.awardee_or_recipient_uniqu),
        ('AwardeeOrRecipientLegalEntityName', subaward.awardee_or_recipient_legal),
        ('Vendor Doing As Business Name', subaward.dba_name),
        ('UltimateParentUniqueIdentifier', subaward.ultimate_parent_unique_ide),
        ('UltimateParentLegalEntityName', subaward.ultimate_parent_legal_enti),
        ('LegalEntityCountryCode', subaward.legal_entity_country_code),
        ('LegalEntityCountryName', subaward.legal_entity_country_name),
        ('LegalEntityAddressLine1', subaward.legal_entity_address_line1),
        ('LegalEntityCityName', subaward.legal_entity_city_name),
        ('LegalEntityStateCode', subaward.legal_entity_state_code),
        ('LegalEntityStateName', subaward.legal_entity_state_name),
        ('LegalEntityZIP+4', subaward.legal_entity_zip),
        ('LegalEntityCongressionalDistrict', subaward.legal_entity_congressional),
        ('LegalEntityForeignPostalCode', subaward.legal_entity_foreign_posta),
        ('PrimeAwardeeBusinessTypes', subaward.business_types),
        ('PrimaryPlaceOfPerformanceCityName', subaward.place_of_perform_city_name),
        ('PrimaryPlaceOfPerformanceStateCode', subaward.place_of_perform_state_code),
        ('PrimaryPlaceOfPerformanceStateName', subaward.place_of_perform_state_name),
        ('PrimaryPlaceOfPerformanceZIP+4', subaward.place_of_performance_zip),
        ('PrimaryPlaceOfPerformanceCongressionalDistrict', subaward.place_of_perform_congressio),
        ('PrimaryPlaceOfPerformanceCountryCode', subaward.place_of_perform_country_co),
        ('PrimaryPlaceOfPerformanceCountryName', subaward.place_of_perform_country_na),
        ('AwardDescription', subaward.award_description),
        ('PrimeAwardProjectTitle', subaward.program_title),
        ('NAICS', subaward.naics),
        ('NAICS_Description', subaward.naics_description),
        ('CFDA_Numbers', subaward.cfda_numbers),
        ('CFDA_Titles', subaward.cfda_titles),
        ('SubAwardType', subaward.subaward_type),
        ('SubAwardReportID', subaward.internal_id),
        ('SubAwardReportYear', subaward.subaward_report_year),
        ('SubAwardReportMonth', subaward.subaward_report_month),
        ('SubAwardNumber', subaward.subaward_number),
        ('SubAwardAmount', subaward.subaward_amount),
        ('SubAwardActionDate', subaward.sub_action_date),
        ('SubAwardeeOrRecipientUniqueIdentifier', subaward.sub_awardee_or_recipient_uniqu),
        ('SubAwardeeOrRecipientLegalEntityName', subaward.sub_awardee_or_recipient_legal),
        ('SubAwardeeDoingBusinessAsName', subaward.sub_dba_name),
        ('SubAwardeeUltimateParentUniqueIdentifier', subaward.sub_ultimate_parent_unique_ide),
        ('SubAwardeeUltimateParentLegalEntityName', subaward.sub_ultimate_parent_legal_enti),
        ('SubAwardeeLegalEntityCountryCode', subaward.sub_legal_entity_country_code),
        ('SubAwardeeLegalEntityCountryName', subaward.sub_legal_entity_country_name),
        ('SubAwardeeLegalEntityAddressLine1', subaward.sub_legal_entity_address_line1),
        ('SubAwardeeLegalEntityCityName', subaward.sub_legal_entity_city_name),
        ('SubAwardeeLegalEntityStateCode', subaward.sub_legal_entity_state_code),
        ('SubAwardeeLegalEntityStateName', subaward.sub_legal_entity_state_name),
        ('SubAwardeeLegalEntityZIP+4', subaward.sub_legal_entity_zip),
        ('SubAwardeeLegalEntityCongressionalDistrict', subaward.sub_legal_entity_congressional),
        ('SubAwardeeLegalEntityForeignPostalCode', subaward.sub_legal_entity_foreign_posta),
        ('SubAwardeeBusinessTypes', subaward.sub_business_types),
        ('SubAwardPlaceOfPerformanceCityName', subaward.sub_place_of_perform_city_name),
        ('SubAwardPlaceOfPerformanceStateCode', subaward.sub_place_of_perform_state_code),
        ('SubAwardPlaceOfPerformanceStateName', subaward.sub_place_of_perform_state_name),
        ('SubAwardPlaceOfPerformanceZIP+4', subaward.sub_place_of_performance_zip),
        ('SubAwardPlaceOfPerformanceCongressionalDistrict', subaward.sub_place_of_perform_congressio),
        ('SubAwardPlaceOfPerformanceCountryCode', subaward.sub_place_of_perform_country_co),
        ('SubAwardPlaceOfPerformanceCountryName', subaward.sub_place_of_perform_country_na),
        ('SubAwardDescription', subaward.subaward_description),
        ('SubAwardeeHighCompOfficer1FullName', subaward.sub_high_comp_officer1_full_na),
        ('SubAwardeeHighCompOfficer1Amount', subaward.sub_high_comp_officer1_amount),
        ('SubAwardeeHighCompOfficer2FullName', subaward.sub_high_comp_officer2_full_na),
        ('SubAwardeeHighCompOfficer2Amount', subaward.sub_high_comp_officer2_amount),
        ('SubAwardeeHighCompOfficer3FullName', subaward.sub_high_comp_officer3_full_na),
        ('SubAwardeeHighCompOfficer3Amount', subaward.sub_high_comp_officer3_amount),
        ('SubAwardeeHighCompOfficer4FullName', subaward.sub_high_comp_officer4_full_na),
        ('SubAwardeeHighCompOfficer4Amount', subaward.sub_high_comp_officer4_amount),
        ('SubAwardeeHighCompOfficer5FullName', subaward.sub_high_comp_officer5_full_na),
        ('SubAwardeeHighCompOfficer5Amount', subaward.sub_high_comp_officer5_amount),
        ('SubAwardReportLastModifiedDate', subaward.date_submitted),
    ])


def test_generate_file_f_sql(database, monkeypatch):
    """ generate_file_f_sql should provide the query representing F file data related to a submission """
    # Setup - create submission, awards, subawards
    sess = database.session

    sub1 = SubmissionFactory(submission_id=1)
    sub2 = SubmissionFactory(submission_id=2)

    d1_show = AwardProcurementFactory(submission_id=sub1.submission_id, piid='PIID1', parent_award_id='PID1',
                                      awarding_sub_tier_agency_c='ASAC1')
    d2_show = AwardFinancialAssistanceFactory(submission_id=sub1.submission_id, fain='FAIN1')
    d1_hide = AwardProcurementFactory(submission_id=sub2.submission_id, piid='PIID2', parent_award_id='PID2',
                                      awarding_sub_tier_agency_c='ASAC2')
    d2_hide = AwardFinancialAssistanceFactory(submission_id=sub2.submission_id, fain='FAIN2')

    sub_contracts_show = [SubawardFactory(id=i, subaward_type='sub-contract', award_id=d1_show.piid,
                                          parent_award_id=d1_show.parent_award_id,
                                          awarding_sub_tier_agency_c=d1_show.awarding_sub_tier_agency_c)
                          for i in range(0, 5)]
    sub_grants_show = [SubawardFactory(id=i, subaward_type='sub-grant', award_id=d2_show.fain) for i in range(5, 10)]
    sub_contracts_hide = [SubawardFactory(id=i, subaward_type='sub-contract', award_id=d1_hide.piid,
                                          parent_award_id=d1_hide.parent_award_id,
                                          awarding_sub_tier_agency_c=d1_hide.awarding_sub_tier_agency_c)
                          for i in range(10, 15)]
    sub_grants_hide = [SubawardFactory(id=i, subaward_type='sub-grant', award_id=d2_hide.fain) for i in range(15, 20)]
    subawards = sub_contracts_show + sub_grants_show + sub_contracts_hide + sub_grants_hide

    sess.add_all([sub1, sub2, d1_hide, d1_show, d2_hide, d2_show] + subawards)
    sess.commit()

    # Gather the sql
    file_f_query = fileE_F.generate_file_f_sql(sub1.submission_id)

    # Get the records
    file_f_records = sess.execute(file_f_query)
    file_f_cols = file_f_records.keys()
    file_f_value_sets = file_f_records.fetchall()
    file_f_results = [OrderedDict(list(zip(file_f_cols, file_f_value_set))) for file_f_value_set in file_f_value_sets]

    # Expected Results
    expected_file_f_results = [replicate_file_f_results(subaward) for subaward in sub_contracts_show + sub_grants_show]

    assert file_f_results == expected_file_f_results
