WITH ap_sub_{0} AS (
    SELECT piid,
        parent_award_id,
        awarding_sub_tier_agency_c
    FROM award_procurement
    WHERE submission_id = {0}),
afa_sub_{0} AS (
    SELECT fain
    FROM award_financial_assistance
    WHERE submission_id = {0}),
submission_awards_{0} AS
    (SELECT *
    FROM subaward
    WHERE EXISTS (SELECT 1
        FROM ap_sub_{0} AS ap
        WHERE subaward.award_id = ap.piid
            AND COALESCE(subaward.parent_award_id, '') = COALESCE(ap.parent_award_id, '')
            AND subaward.awarding_sub_tier_agency_c = ap.awarding_sub_tier_agency_c
            AND subaward.subaward_type = 'sub-contract'
    )
    UNION
    SELECT *
    FROM subaward
    WHERE EXISTS (SELECT 1
        FROM afa_sub_{0} AS afa
        WHERE subaward.award_id = afa.fain
            AND subaward.subaward_type = 'sub-grant'
    ))
SELECT
    unique_award_key AS "PrimeAwardUniqueKey",
    award_id AS "PrimeAwardID",
    parent_award_id AS "ParentAwardID",
    award_amount AS "PrimeAwardAmount",
    action_date AS "ActionDate",
    fy AS "PrimeAwardFiscalYear",
    awarding_agency_code AS "AwardingAgencyCode",
    awarding_agency_name AS "AwardingAgencyName",
    awarding_sub_tier_agency_c AS "AwardingSubTierAgencyCode",
    awarding_sub_tier_agency_n AS "AwardingSubTierAgencyName",
    awarding_office_code AS "AwardingOfficeCode",
    awarding_office_name AS "AwardingOfficeName",
    funding_agency_code AS "FundingAgencyCode",
    funding_agency_name AS "FundingAgencyName",
    funding_sub_tier_agency_co AS "FundingSubTierAgencyCode",
    funding_sub_tier_agency_na AS "FundingSubTierAgencyName",
    funding_office_code AS "FundingOfficeCode",
    funding_office_name AS "FundingOfficeName",
    awardee_or_recipient_uniqu AS "AwardeeOrRecipientUniqueIdentifier",
    awardee_or_recipient_legal AS "AwardeeOrRecipientLegalEntityName",
    dba_name AS "Vendor Doing As Business Name",
    ultimate_parent_unique_ide AS "UltimateParentUniqueIdentifier",
    ultimate_parent_legal_enti AS "UltimateParentLegalEntityName",
    legal_entity_country_code AS "LegalEntityCountryCode",
    legal_entity_country_name AS "LegalEntityCountryName",
    legal_entity_address_line1 AS "LegalEntityAddressLine1",
    legal_entity_city_name AS "LegalEntityCityName",
    legal_entity_state_code AS "LegalEntityStateCode",
    legal_entity_state_name AS "LegalEntityStateName",
    legal_entity_zip AS "LegalEntityZIP+4",
    legal_entity_congressional AS "LegalEntityCongressionalDistrict",
    legal_entity_foreign_posta AS "LegalEntityForeignPostalCode",
    business_types AS "PrimeAwardeeBusinessTypes",
    place_of_perform_city_name AS "PrimaryPlaceOfPerformanceCityName",
    place_of_perform_state_code AS "PrimaryPlaceOfPerformanceStateCode",
    place_of_perform_state_name AS "PrimaryPlaceOfPerformanceStateName",
    place_of_performance_zip AS "PrimaryPlaceOfPerformanceZIP+4",
    place_of_perform_congressio AS "PrimaryPlaceOfPerformanceCongressionalDistrict",
    place_of_perform_country_co AS "PrimaryPlaceOfPerformanceCountryCode",
    place_of_perform_country_na AS "PrimaryPlaceOfPerformanceCountryName",
    award_description AS "AwardDescription",
    naics AS "NAICS",
    naics_description AS "NAICS_Description",
    cfda_numbers AS "CFDA_Numbers",
    cfda_titles AS "CFDA_Titles",

    subaward_type AS "SubAwardType",
    subaward_report_year AS "SubAwardReportYear",
    subaward_report_month AS "SubAwardReportMonth",
    subaward_number AS "SubAwardNumber",
    subaward_amount AS "SubAwardAmount",
    sub_action_date AS "SubAwardActionDate",
    sub_awardee_or_recipient_uniqu AS "SubAwardeeOrRecipientUniqueIdentifier",
    sub_awardee_or_recipient_legal AS "SubAwardeeOrRecipientLegalEntityName",
    sub_dba_name AS "SubAwardeeDoingBusinessAsName",
    sub_ultimate_parent_unique_ide AS "SubAwardeeUltimateParentUniqueIdentifier",
    sub_ultimate_parent_legal_enti AS "SubAwardeeUltimateParentLegalEntityName",
    sub_legal_entity_country_code AS "SubAwardeeLegalEntityCountryCode",
    sub_legal_entity_country_name AS "SubAwardeeLegalEntityCountryName",
    sub_legal_entity_address_line1 AS "SubAwardeeLegalEntityAddressLine1",
    sub_legal_entity_city_name AS "SubAwardeeLegalEntityCityName",
    sub_legal_entity_state_code AS "SubAwardeeLegalEntityStateCode",
    sub_legal_entity_state_name AS "SubAwardeeLegalEntityStateName",
    sub_legal_entity_zip AS "SubAwardeeLegalEntityZIP+4",
    sub_legal_entity_congressional AS "SubAwardeeLegalEntityCongressionalDistrict",
    sub_legal_entity_foreign_posta AS "SubAwardeeLegalEntityForeignPostalCode",
    sub_business_types AS "SubAwardeeBusinessTypes",
    sub_place_of_perform_city_name AS "SubAwardPlaceOfPerformanceCityName",
    sub_place_of_perform_state_code AS "SubAwardPlaceOfPerformanceStateCode",
    sub_place_of_perform_state_name AS "SubAwardPlaceOfPerformanceStateName",
    sub_place_of_performance_zip AS "SubAwardPlaceOfPerformanceZIP+4",
    sub_place_of_perform_congressio AS "SubAwardPlaceOfPerformanceCongressionalDistrict",
    sub_place_of_perform_country_co AS "SubAwardPlaceOfPerformanceCountryCode",
    sub_place_of_perform_country_na AS "SubAwardPlaceOfPerformanceCountryName",
    subaward_description AS "SubAwardDescription",
    sub_high_comp_officer1_full_na AS "SubAwardeeHighCompOfficer1FullName",
    sub_high_comp_officer1_amount AS "SubAwardeeHighCompOfficer1Amount",
    sub_high_comp_officer2_full_na AS "SubAwardeeHighCompOfficer2FullName",
    sub_high_comp_officer2_amount AS "SubAwardeeHighCompOfficer2Amount",
    sub_high_comp_officer3_full_na AS "SubAwardeeHighCompOfficer3FullName",
    sub_high_comp_officer3_amount AS "SubAwardeeHighCompOfficer3Amount",
    sub_high_comp_officer4_full_na AS "SubAwardeeHighCompOfficer4FullName",
    sub_high_comp_officer4_amount AS "SubAwardeeHighCompOfficer4Amount",
    sub_high_comp_officer5_full_na AS "SubAwardeeHighCompOfficer5FullName",
    sub_high_comp_officer5_amount AS "SubAwardeeHighCompOfficer5Amount"
FROM submission_awards_{0}
