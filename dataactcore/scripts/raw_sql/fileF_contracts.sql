WITH ap_sub AS
    (SELECT DISTINCT ON (
            award_procurement.piid,
            award_procurement.parent_award_id,
            award_procurement.submission_id
        )
        award_procurement.piid AS piid,
        award_procurement.idv_type AS idv_type,
        award_procurement.parent_award_id AS parent_award_id,
        award_procurement.award_description as award_description,
        award_procurement.submission_id AS submission_id,
        award_procurement.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
        award_procurement.naics_description AS naics_description,
        award_procurement.awarding_agency_code AS awarding_agency_code,
        award_procurement.awarding_agency_name AS awarding_agency_name,
        award_procurement.funding_agency_code AS funding_agency_code,
        award_procurement.funding_agency_name AS funding_agency_name
    FROM award_procurement
    WHERE award_procurement.submission_id = {0})
SELECT
    CASE WHEN ap_sub.idv_type IS NOT NULL
        THEN UPPER('CONT_IDV_' ||
            COALESCE(fsrs_procurement.contract_number, '-NONE-') || '_' ||
            COALESCE(fsrs_procurement.contract_agency_code, '-NONE-'))
        ELSE UPPER('CONT_AWD_' ||
            COALESCE(fsrs_procurement.contract_number, '-NONE-') || '_' ||
            COALESCE(fsrs_procurement.contract_agency_code, '-NONE-') || '_' ||
            COALESCE(fsrs_procurement.contract_idv_agency_code, '-NONE-') || '_' ||
            COALESCE(fsrs_procurement.idv_reference_number, '-NONE-'))
    END AS "PrimeAwardUniqueKey",
    fsrs_procurement.contract_number AS "PrimeAwardID",
    fsrs_procurement.idv_reference_number AS "ParentAwardID",
    fsrs_procurement.dollar_obligated AS "PrimeAwardAmount",
    fsrs_procurement.date_signed AS "ActionDate",
    'FY' || fy(fsrs_procurement.date_signed) AS "PrimeAwardFiscalYear",
    ap_sub.awarding_agency_code AS "AwardingAgencyCode",
    ap_sub.awarding_agency_name AS "AwardingAgencyName",
    fsrs_procurement.contracting_office_aid AS "AwardingSubTierAgencyCode",
    fsrs_procurement.contracting_office_aname AS "AwardingSubTierAgencyName",
    fsrs_procurement.contracting_office_id AS "AwardingOfficeCode",
    fsrs_procurement.contracting_office_name AS "AwardingOfficeName",
    ap_sub.funding_agency_code AS "FundingAgencyCode",
    ap_sub.funding_agency_name AS "FundingAgencyName",
    fsrs_procurement.funding_agency_id AS "FundingSubTierAgencyCode",
    fsrs_procurement.funding_agency_name AS "FundingSubTierAgencyName",
    fsrs_procurement.funding_office_id AS "FundingOfficeCode",
    fsrs_procurement.funding_office_name AS "FundingOfficeName",
    fsrs_procurement.duns AS "AwardeeOrRecipientUniqueIdentifier",
    fsrs_procurement.company_name AS "AwardeeOrRecipientLegalEntityName",
    fsrs_procurement.dba_name AS "Vendor Doing As Business Name",
    fsrs_procurement.parent_duns AS "UltimateParentUniqueIdentifier",
    fsrs_procurement.parent_company_name AS "UltimateParentLegalEntityName",
    fsrs_procurement.company_address_country AS "LegalEntityCountryCode",
    le_country.country_name AS "LegalEntityCountryName",
    fsrs_procurement.company_address_street AS "LegalEntityAddressLine1",
    fsrs_procurement.company_address_city AS "LegalEntityCityName",
    fsrs_procurement.company_address_state AS "LegalEntityStateCode",
    fsrs_procurement.company_address_state_name AS "LegalEntityStateName",
    CASE WHEN fsrs_procurement.company_address_country = 'USA'
        THEN fsrs_procurement.company_address_zip
        ELSE NULL
    END AS "LegalEntityZIP+4",
    fsrs_procurement.company_address_district AS "LegalEntityCongressionalDistrict",
    CASE WHEN fsrs_procurement.company_address_country <> 'USA'
        THEN fsrs_procurement.company_address_zip
        ELSE NULL
    END AS "LegalEntityForeignPostalCode",
    fsrs_procurement.bus_types AS "PrimeAwardeeBusinessTypes",
    fsrs_procurement.principle_place_city AS "PrimaryPlaceOfPerformanceCityName",
    fsrs_procurement.principle_place_state AS "PrimaryPlaceOfPerformanceStateCode",
    fsrs_procurement.principle_place_state_name AS "PrimaryPlaceOfPerformanceStateName",
    fsrs_procurement.principle_place_zip AS "PrimaryPlaceOfPerformanceZIP+4",
    fsrs_procurement.principle_place_district AS "PrimaryPlaceOfPerformanceCongressionalDistrict",
    fsrs_procurement.principle_place_country AS "PrimaryPlaceOfPerformanceCountryCode",
    ppop_country.country_name AS "PrimaryPlaceOfPerformanceCountryName",
    ap_sub.award_description AS "AwardDescription",
    fsrs_procurement.naics AS "NAICS",
    ap_sub.naics_description AS "NAICS_Description",
    NULL AS "CFDA_Numbers",
    NULL AS "CFDA_Titles",

    'sub-contract' AS "SubAwardType",
    fsrs_procurement.report_period_year AS "SubAwardReportYear",
    fsrs_procurement.report_period_mon AS "SubAwardReportMonth",
    fsrs_subcontract.subcontract_num AS "SubAwardNumber",
    fsrs_subcontract.subcontract_amount AS "SubAwardAmount",
    fsrs_subcontract.subcontract_date AS "SubAwardActionDate",
    fsrs_subcontract.duns AS "SubAwardeeOrRecipientUniqueIdentifier",
    fsrs_subcontract.company_name AS "SubAwardeeOrRecipientLegalEntityName",
    fsrs_subcontract.dba_name AS "SubAwardeeDoingBusinessAsName",
    fsrs_subcontract.parent_duns AS "SubAwardeeUltimateParentUniqueIdentifier",
    fsrs_subcontract.parent_company_name AS "SubAwardeeUltimateParentLegalEntityName",
    fsrs_subcontract.company_address_country AS "SubAwardeeLegalEntityCountryCode",
    sub_le_country.country_name AS "SubAwardeeLegalEntityCountryName",
    fsrs_subcontract.company_address_street AS "SubAwardeeLegalEntityAddressLine1",
    fsrs_subcontract.company_address_city AS "SubAwardeeLegalEntityCityName",
    fsrs_subcontract.company_address_state AS "SubAwardeeLegalEntityStateCode",
    fsrs_subcontract.company_address_state_name AS "SubAwardeeLegalEntityStateName",
    CASE WHEN fsrs_subcontract.company_address_country = 'USA'
        THEN fsrs_subcontract.company_address_zip
        ELSE NULL
    END AS "SubAwardeeLegalEntityZIP+4",
    fsrs_subcontract.company_address_district AS "SubAwardeeLegalEntityCongressionalDistrict",
    CASE WHEN fsrs_subcontract.company_address_country <> 'USA'
        THEN fsrs_subcontract.company_address_zip
        ELSE NULL
    END AS "SubAwardeeLegalEntityForeignPostalCode",
    fsrs_subcontract.bus_types AS "SubAwardeeBusinessTypes",
    fsrs_subcontract.principle_place_city AS "SubAwardPlaceOfPerformanceCityName",
    fsrs_subcontract.principle_place_state AS "SubAwardPlaceOfPerformanceStateCode",
    fsrs_subcontract.principle_place_state_name AS "SubAwardPlaceOfPerformanceStateName",
    fsrs_subcontract.principle_place_zip AS "SubAwardPlaceOfPerformanceZIP+4",
    fsrs_subcontract.principle_place_district AS "SubAwardPlaceOfPerformanceCongressionalDistrict",
    fsrs_subcontract.principle_place_country AS "SubAwardPlaceOfPerformanceCountryCode",
    sub_ppop_country.country_name AS "SubAwardPlaceOfPerformanceCountryName",
    fsrs_subcontract.overall_description AS "SubAwardDescription",
    fsrs_subcontract.top_paid_fullname_1 AS "SubAwardeeHighCompOfficer1FullName",
    fsrs_subcontract.top_paid_amount_1 AS "SubAwardeeHighCompOfficer1Amount",
    fsrs_subcontract.top_paid_fullname_2 AS "SubAwardeeHighCompOfficer2FullName",
    fsrs_subcontract.top_paid_amount_2 AS "SubAwardeeHighCompOfficer2Amount",
    fsrs_subcontract.top_paid_fullname_3 AS "SubAwardeeHighCompOfficer3FullName",
    fsrs_subcontract.top_paid_amount_3 AS "SubAwardeeHighCompOfficer3Amount",
    fsrs_subcontract.top_paid_fullname_4 AS "SubAwardeeHighCompOfficer4FullName",
    fsrs_subcontract.top_paid_amount_4 AS "SubAwardeeHighCompOfficer4Amount",
    fsrs_subcontract.top_paid_fullname_5 AS "SubAwardeeHighCompOfficer5FullName",
    fsrs_subcontract.top_paid_amount_5 AS "SubAwardeeHighCompOfficer5Amount"
FROM ap_sub
    JOIN fsrs_procurement
        ON fsrs_procurement.contract_number = ap_sub.piid
        AND fsrs_procurement.idv_reference_number IS NOT DISTINCT FROM ap_sub.parent_award_id
        AND fsrs_procurement.contracting_office_aid = ap_sub.awarding_sub_tier_agency_c
    JOIN fsrs_subcontract
        ON fsrs_subcontract.parent_id = fsrs_procurement.id
    LEFT OUTER JOIN country_code AS le_country
        ON fsrs_procurement.company_address_country = le_country.country_code
    LEFT OUTER JOIN country_code AS ppop_country
        ON fsrs_procurement.principle_place_country = ppop_country.country_code
    LEFT OUTER JOIN country_code AS sub_le_country
        ON fsrs_subcontract.company_address_country = sub_le_country.country_code
    LEFT OUTER JOIN country_code AS sub_ppop_country
        ON fsrs_subcontract.principle_place_country = sub_ppop_country.country_code
