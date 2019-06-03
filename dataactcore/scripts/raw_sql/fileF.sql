WITH submission_awards_{0} AS
    (SELECT DISTINCT(unique_award_key)
    FROM (
        SELECT
            dap.unique_award_key
        FROM award_procurement AS ap
        JOIN detached_award_procurement AS dap
            ON dap.detached_award_proc_unique = ap.detached_award_proc_unique
        WHERE ap.submission_id = {0}
        UNION
        SELECT
            dafa.unique_award_key
        FROM award_financial_assistance AS afa
        JOIN detached_award_financial_assistance AS dafa
            ON dafa.afa_generated_unique = afa.afa_generated_unique
        WHERE afa.submission_id = {0}) AS temp)
SELECT
    subaward.unique_award_key AS "PrimeAwardUniqueKey",
    subaward.award_id AS "PrimeAwardID",
    subaward.parent_award_id AS "ParentAwardID",
    subaward.award_amount AS "PrimeAwardAmount",
    subaward.action_date AS "ActionDate",
    subaward.fy AS "PrimeAwardFiscalYear",
    subaward.awarding_agency_code AS "AwardingAgencyCode",
    subaward.awarding_agency_name AS "AwardingAgencyName",
    subaward.awarding_sub_tier_agency_c AS "AwardingSubTierAgencyCode",
    subaward.awarding_sub_tier_agency_n AS "AwardingSubTierAgencyName",
    subaward.awarding_office_code AS "AwardingOfficeCode",
    subaward.awarding_office_name AS "AwardingOfficeName",
    subaward.funding_agency_code AS "FundingAgencyCode",
    subaward.funding_agency_name AS "FundingAgencyName",
    subaward.funding_sub_tier_agency_co AS "FundingSubTierAgencyCode",
    subaward.funding_sub_tier_agency_na AS "FundingSubTierAgencyName",
    subaward.funding_office_code AS "FundingOfficeCode",
    subaward.funding_office_name AS "FundingOfficeName",
    subaward.awardee_or_recipient_uniqu AS "AwardeeOrRecipientUniqueIdentifier",
    subaward.awardee_or_recipient_legal AS "AwardeeOrRecipientLegalEntityName",
    subaward.dba_name AS "Vendor Doing As Business Name",
    subaward.ultimate_parent_unique_ide AS "UltimateParentUniqueIdentifier",
    subaward.ultimate_parent_legal_enti AS "UltimateParentLegalEntityName",
    subaward.legal_entity_country_code AS "LegalEntityCountryCode",
    subaward.legal_entity_country_name AS "LegalEntityCountryName",
    subaward.legal_entity_address_line1 AS "LegalEntityAddressLine1",
    subaward.legal_entity_city_name AS "LegalEntityCityName",
    subaward.legal_entity_state_code AS "LegalEntityStateCode",
    subaward.legal_entity_state_name AS "LegalEntityStateName",
    subaward.legal_entity_zip AS "LegalEntityZIP+4",
    subaward.legal_entity_congressional AS "LegalEntityCongressionalDistrict",
    subaward.legal_entity_foreign_posta AS "LegalEntityForeignPostalCode",
    subaward.business_types AS "PrimeAwardeeBusinessTypes",
    subaward.place_of_perform_city_name AS "PrimaryPlaceOfPerformanceCityName",
    subaward.place_of_perform_state_code AS "PrimaryPlaceOfPerformanceStateCode",
    subaward.place_of_perform_state_name AS "PrimaryPlaceOfPerformanceStateName",
    subaward.place_of_performance_zip AS "PrimaryPlaceOfPerformanceZIP+4",
    subaward.place_of_perform_congressio AS "PrimaryPlaceOfPerformanceCongressionalDistrict",
    subaward.place_of_perform_country_co AS "PrimaryPlaceOfPerformanceCountryCode",
    subaward.place_of_perform_country_na AS "PrimaryPlaceOfPerformanceCountryName",
    subaward.award_description AS "AwardDescription",
    subaward.naics AS "NAICS",
    subaward.naics_description AS "NAICS_Description",
    subaward.cfda_numbers AS "CFDA_Numbers",
    subaward.cfda_titles AS "CFDA_Titles",

    subaward.subaward_type AS "SubAwardType",
    subaward.subaward_report_year AS "SubAwardReportYear",
    subaward.subaward_report_month AS "SubAwardReportMonth",
    subaward.subaward_number AS "SubAwardNumber",
    subaward.subaward_amount AS "SubAwardAmount",
    subaward.sub_action_date AS "SubAwardActionDate",
    subaward.sub_awardee_or_recipient_uniqu AS "SubAwardeeOrRecipientUniqueIdentifier",
    subaward.sub_awardee_or_recipient_legal AS "SubAwardeeOrRecipientLegalEntityName",
    subaward.sub_dba_name AS "SubAwardeeDoingBusinessAsName",
    subaward.sub_ultimate_parent_unique_ide AS "SubAwardeeUltimateParentUniqueIdentifier",
    subaward.sub_ultimate_parent_legal_enti AS "SubAwardeeUltimateParentLegalEntityName",
    subaward.sub_legal_entity_country_code AS "SubAwardeeLegalEntityCountryCode",
    subaward.sub_legal_entity_country_name AS "SubAwardeeLegalEntityCountryName",
    subaward.sub_legal_entity_address_line1 AS "SubAwardeeLegalEntityAddressLine1",
    subaward.sub_legal_entity_city_name AS "SubAwardeeLegalEntityCityName",
    subaward.sub_legal_entity_state_code AS "SubAwardeeLegalEntityStateCode",
    subaward.sub_legal_entity_state_name AS "SubAwardeeLegalEntityStateName",
    subaward.sub_legal_entity_zip AS "SubAwardeeLegalEntityZIP+4",
    subaward.sub_legal_entity_congressional AS "SubAwardeeLegalEntityCongressionalDistrict",
    subaward.sub_legal_entity_foreign_posta AS "SubAwardeeLegalEntityForeignPostalCode",
    subaward.sub_business_types AS "SubAwardeeBusinessTypes",
    subaward.sub_place_of_perform_city_name AS "SubAwardPlaceOfPerformanceCityName",
    subaward.sub_place_of_perform_state_code AS "SubAwardPlaceOfPerformanceStateCode",
    subaward.sub_place_of_perform_state_name AS "SubAwardPlaceOfPerformanceStateName",
    subaward.sub_place_of_performance_zip AS "SubAwardPlaceOfPerformanceZIP+4",
    subaward.sub_place_of_perform_congressio AS "SubAwardPlaceOfPerformanceCongressionalDistrict",
    subaward.sub_place_of_perform_country_co AS "SubAwardPlaceOfPerformanceCountryCode",
    subaward.sub_place_of_perform_country_na AS "SubAwardPlaceOfPerformanceCountryName",
    subaward.subaward_description AS "SubAwardDescription",
    subaward.sub_high_comp_officer1_full_na AS "SubAwardeeHighCompOfficer1FullName",
    subaward.sub_high_comp_officer1_amount AS "SubAwardeeHighCompOfficer1Amount",
    subaward.sub_high_comp_officer2_full_na AS "SubAwardeeHighCompOfficer2FullName",
    subaward.sub_high_comp_officer2_amount AS "SubAwardeeHighCompOfficer2Amount",
    subaward.sub_high_comp_officer3_full_na AS "SubAwardeeHighCompOfficer3FullName",
    subaward.sub_high_comp_officer3_amount AS "SubAwardeeHighCompOfficer3Amount",
    subaward.sub_high_comp_officer4_full_na AS "SubAwardeeHighCompOfficer4FullName",
    subaward.sub_high_comp_officer4_amount AS "SubAwardeeHighCompOfficer4Amount",
    subaward.sub_high_comp_officer5_full_na AS "SubAwardeeHighCompOfficer5FullName",
    subaward.sub_high_comp_officer5_amount AS "SubAwardeeHighCompOfficer5Amount"
FROM submission_awards_{0}
    JOIN subaward
        ON subaward.unique_award_key = submission_awards_{0}.unique_award_key
