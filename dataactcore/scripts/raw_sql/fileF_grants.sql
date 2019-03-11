WITH afa_sub AS
    (SELECT DISTINCT ON (
            award_financial_assistance.fain,
            award_financial_assistance.submission_id
        )
        award_financial_assistance.fain AS fain,
        award_financial_assistance.uri AS uri,
        award_financial_assistance.submission_id AS submission_id,
        award_financial_assistance.award_description AS award_description,
        award_financial_assistance.record_type AS record_type,
        award_financial_assistance.awarding_agency_code AS awarding_agency_code,
        award_financial_assistance.awarding_agency_name AS awarding_agency_name,
        award_financial_assistance.awarding_office_code AS awarding_office_code,
        award_financial_assistance.awarding_office_name AS awarding_office_name,
        award_financial_assistance.funding_agency_code AS funding_agency_code,
        award_financial_assistance.funding_agency_name AS funding_agency_name,
        award_financial_assistance.funding_office_code AS funding_office_code,
        award_financial_assistance.funding_office_name AS funding_office_name,
        award_financial_assistance.business_types_desc AS business_types_desc,
        award_financial_assistance.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
        award_financial_assistance.awarding_sub_tier_agency_n AS awarding_sub_tier_agency_n,
        award_financial_assistance.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
        award_financial_assistance.funding_sub_tier_agency_na AS funding_sub_tier_agency_na
    FROM award_financial_assistance
    WHERE award_financial_assistance.submission_id = {0}),
grant_pduns AS
    (SELECT grand_pduns_from.awardee_or_recipient_uniqu AS awardee_or_recipient_uniqu,
        grand_pduns_from.legal_business_name AS legal_business_name
    FROM (
        SELECT duns.awardee_or_recipient_uniqu AS awardee_or_recipient_uniqu,
            duns.legal_business_name AS legal_business_name,
            row_number() OVER (PARTITION BY
                duns.awardee_or_recipient_uniqu
            ) AS row
        FROM fsrs_grant
            LEFT OUTER JOIN duns
                ON fsrs_grant.parent_duns = duns.awardee_or_recipient_uniqu
        ORDER BY duns.activation_date DESC
     ) AS grand_pduns_from
    WHERE grand_pduns_from.row = 1),
subgrant_pduns AS (
    SELECT sub_pduns_from.awardee_or_recipient_uniqu AS awardee_or_recipient_uniqu,
        sub_pduns_from.legal_business_name AS legal_business_name
    FROM (
        SELECT duns.awardee_or_recipient_uniqu AS awardee_or_recipient_uniqu,
            duns.legal_business_name AS legal_business_name,
            row_number() OVER (PARTITION BY
                duns.awardee_or_recipient_uniqu
            ) AS row
        FROM fsrs_subgrant
            LEFT OUTER JOIN duns
                ON fsrs_subgrant.parent_duns = duns.awardee_or_recipient_uniqu
        ORDER BY duns.activation_date DESC
    ) AS sub_pduns_from
    WHERE sub_pduns_from.row = 1),
subgrant_duns AS (
    SELECT sub_duns_from.awardee_or_recipient_uniqu AS awardee_or_recipient_uniqu,
        sub_duns_from.business_types_codes AS business_types_codes
    FROM (
        SELECT
            duns.awardee_or_recipient_uniqu AS awardee_or_recipient_uniqu,
            duns.business_types_codes AS business_types_codes,
            row_number() OVER (PARTITION BY
                duns.awardee_or_recipient_uniqu
            ) AS row
        FROM fsrs_subgrant
            LEFT OUTER JOIN duns
                ON fsrs_subgrant.duns = duns.awardee_or_recipient_uniqu
        ORDER BY duns.activation_date DESC
    ) AS sub_duns_from
    WHERE sub_duns_from.row = 1)
SELECT
    CASE WHEN afa_sub.record_type = '1'
        THEN UPPER('ASST_AGG_' ||
            COALESCE(afa_sub.uri, '-NONE-') || '_' ||
            COALESCE(fsrs_grant.federal_agency_id, '-NONE-'))
        ELSE UPPER('ASST_NON_' ||
            COALESCE(fsrs_grant.fain, '-NONE-') || '_' ||
            COALESCE(fsrs_grant.federal_agency_id, '-NONE-'))
    END AS "PrimeAwardUniqueKey",
    fsrs_grant.fain AS "PrimeAwardID",
    NULL AS "ParentAwardID",
    fsrs_grant.total_fed_funding_amount AS "PrimeAwardAmount",
    fsrs_grant.obligation_date AS "ActionDate",
    'FY' || fy(obligation_date) AS "PrimeAwardFiscalYear",
    afa_sub.awarding_agency_code AS "AwardingAgencyCode",
    afa_sub.awarding_agency_name AS "AwardingAgencyName",
    fsrs_grant.federal_agency_id AS "AwardingSubTierAgencyCode",
    afa_sub.awarding_sub_tier_agency_n AS "AwardingSubTierAgencyName",
    afa_sub.awarding_office_code AS "AwardingOfficeCode",
    afa_sub.awarding_office_name AS "AwardingOfficeName",
    afa_sub.funding_agency_code AS "FundingAgencyCode",
    afa_sub.funding_agency_name AS "FundingAgencyName",
    afa_sub.funding_sub_tier_agency_co AS "FundingSubTierAgencyCode",
    afa_sub.funding_sub_tier_agency_na AS "FundingSubTierAgencyName",
    afa_sub.funding_office_code AS "FundingOfficeCode",
    afa_sub.funding_office_name AS "FundingOfficeName",
    fsrs_grant.duns AS "AwardeeOrRecipientUniqueIdentifier",
    fsrs_grant.awardee_name AS "AwardeeOrRecipientLegalEntityName",
    fsrs_grant.dba_name AS "Vendor Doing As Business Name",
    fsrs_grant.parent_duns AS "UltimateParentUniqueIdentifier",
    grant_pduns.legal_business_name AS "UltimateParentLegalEntityName",
    fsrs_grant.awardee_address_country AS "LegalEntityCountryCode",
    le_country.country_name AS "LegalEntityCountryName",
    fsrs_grant.awardee_address_street AS "LegalEntityAddressLine1",
    fsrs_grant.awardee_address_city AS "LegalEntityCityName",
    fsrs_grant.awardee_address_state AS "LegalEntityStateCode",
    fsrs_grant.awardee_address_state_name AS "LegalEntityStateName",
    CASE WHEN fsrs_grant.awardee_address_country = 'USA'
        THEN fsrs_grant.awardee_address_zip
        ELSE NULL
    END AS "LegalEntityZIP+4",
    fsrs_grant.awardee_address_district AS "LegalEntityCongressionalDistrict",
    CASE WHEN fsrs_grant.awardee_address_country <> 'USA'
        THEN fsrs_grant.awardee_address_zip
        ELSE NULL
    END AS "LegalEntityForeignPostalCode",
    afa_sub.business_types_desc AS "PrimeAwardeeBusinessTypes",
    fsrs_grant.principle_place_city AS "PrimaryPlaceOfPerformanceCityName",
    fsrs_grant.principle_place_state AS "PrimaryPlaceOfPerformanceStateCode",
    fsrs_grant.principle_place_state_name AS "PrimaryPlaceOfPerformanceStateName",
    fsrs_grant.principle_place_zip AS "PrimaryPlaceOfPerformanceZIP+4",
    fsrs_grant.principle_place_district AS "PrimaryPlaceOfPerformanceCongressionalDistrict",
    fsrs_grant.principle_place_country AS "PrimaryPlaceOfPerformanceCountryCode",
    ppop_country.country_name AS "PrimaryPlaceOfPerformanceCountryName",
    fsrs_grant.project_description AS "AwardDescription",
    NULL AS "NAICS",
    NULL AS "NAICS_Description",
    CASE WHEN fsrs_grant.cfda_numbers ~ ';'
        THEN cfda_num_loop(fsrs_grant.cfda_numbers)
        ELSE cfda_num(fsrs_grant.cfda_numbers)
    END AS "CFDA_Numbers",
    CASE WHEN fsrs_grant.cfda_numbers ~ ';'
        THEN cfda_word_loop(fsrs_grant.cfda_numbers)
        ELSE cfda_word(fsrs_grant.cfda_numbers)
    END AS "CFDA_Titles",

    'sub-grant' AS "SubAwardType",
    fsrs_grant.report_period_year AS "SubAwardReportYear",
    fsrs_grant.report_period_mon AS "SubAwardReportMonth",
    fsrs_subgrant.subaward_num AS "SubAwardNumber",
    fsrs_subgrant.subaward_amount AS "SubAwardAmount",
    fsrs_subgrant.subaward_date AS "SubAwardActionDate",
    fsrs_subgrant.duns AS "SubAwardeeOrRecipientUniqueIdentifier",
    fsrs_subgrant.awardee_name AS "SubAwardeeOrRecipientLegalEntityName",
    fsrs_subgrant.dba_name AS "SubAwardeeDoingBusinessAsName",
    fsrs_subgrant.parent_duns AS "SubAwardeeUltimateParentUniqueIdentifier",
    subgrant_pduns.legal_business_name AS "SubAwardeeUltimateParentLegalEntityName",
    fsrs_subgrant.awardee_address_country AS "SubAwardeeLegalEntityCountryCode",
    sub_le_country.country_name AS "SubAwardeeLegalEntityCountryName",
    fsrs_subgrant.awardee_address_street AS "SubAwardeeLegalEntityAddressLine1",
    fsrs_subgrant.awardee_address_city AS "SubAwardeeLegalEntityCityName",
    fsrs_subgrant.awardee_address_state AS "SubAwardeeLegalEntityStateCode",
    fsrs_subgrant.awardee_address_state_name AS "SubAwardeeLegalEntityStateName",
    CASE WHEN fsrs_subgrant.awardee_address_country = 'USA'
        THEN fsrs_subgrant.awardee_address_zip
        ELSE NULL
    END AS "SubAwardeeLegalEntityZIP+4",
    fsrs_subgrant.awardee_address_district AS "SubAwardeeLegalEntityCongressionalDistrict",
    CASE WHEN fsrs_subgrant.awardee_address_country <> 'USA'
        THEN fsrs_subgrant.awardee_address_zip
        ELSE NULL
    END AS "SubAwardeeLegalEntityForeignPostalCode",
    array_to_string(subgrant_duns.business_types_codes, ', ') AS "SubAwardeeBusinessTypes",
    fsrs_subgrant.principle_place_city AS "SubAwardPlaceOfPerformanceCityName",
    fsrs_subgrant.principle_place_state AS "SubAwardPlaceOfPerformanceStateCode",
    fsrs_subgrant.principle_place_state_name AS "SubAwardPlaceOfPerformanceStateName",
    fsrs_subgrant.principle_place_zip AS "SubAwardPlaceOfPerformanceZIP+4",
    fsrs_subgrant.principle_place_district AS "SubAwardPlaceOfPerformanceCongressionalDistrict",
    fsrs_subgrant.principle_place_country AS "SubAwardPlaceOfPerformanceCountryCode",
    sub_ppop_country.country_name AS "SubAwardPlaceOfPerformanceCountryName",
    fsrs_subgrant.project_description AS "SubAwardDescription",
    fsrs_subgrant.top_paid_fullname_1 AS "SubAwardeeHighCompOfficer1FullName",
    fsrs_subgrant.top_paid_amount_1 AS "SubAwardeeHighCompOfficer1Amount",
    fsrs_subgrant.top_paid_fullname_2 AS "SubAwardeeHighCompOfficer2FullName",
    fsrs_subgrant.top_paid_amount_2 AS "SubAwardeeHighCompOfficer2Amount",
    fsrs_subgrant.top_paid_fullname_3 AS "SubAwardeeHighCompOfficer3FullName",
    fsrs_subgrant.top_paid_amount_3 AS "SubAwardeeHighCompOfficer3Amount",
    fsrs_subgrant.top_paid_fullname_4 AS "SubAwardeeHighCompOfficer4FullName",
    fsrs_subgrant.top_paid_amount_4 AS "SubAwardeeHighCompOfficer4Amount",
    fsrs_subgrant.top_paid_fullname_5 AS "SubAwardeeHighCompOfficer5FullName",
    fsrs_subgrant.top_paid_amount_5 AS "SubAwardeeHighCompOfficer5Amount"
FROM afa_sub
    JOIN fsrs_grant
        ON fsrs_grant.fain = afa_sub.fain
    JOIN fsrs_subgrant
        ON fsrs_subgrant.parent_id = fsrs_grant.id
    LEFT OUTER JOIN country_code AS le_country
        ON fsrs_grant.awardee_address_country = le_country.country_code
    LEFT OUTER JOIN country_code AS ppop_country
        ON fsrs_grant.principle_place_country = ppop_country.country_code
    LEFT OUTER JOIN country_code AS sub_le_country
        ON fsrs_subgrant.awardee_address_country = sub_le_country.country_code
    LEFT OUTER JOIN country_code AS sub_ppop_country
        ON fsrs_subgrant.principle_place_country = sub_ppop_country.country_code
    LEFT OUTER JOIN grant_pduns
        ON fsrs_grant.parent_duns = grant_pduns.awardee_or_recipient_uniqu
    LEFT OUTER JOIN subgrant_pduns
        ON fsrs_subgrant.parent_duns = subgrant_pduns.awardee_or_recipient_uniqu
    LEFT OUTER JOIN subgrant_duns
        ON fsrs_subgrant.duns = subgrant_duns.awardee_or_recipient_uniqu
