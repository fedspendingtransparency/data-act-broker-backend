WITH aw_pafa AS
    (SELECT DISTINCT ON (
            pafa.fain
        )
        pafa.fain AS fain,
        pafa.uri AS uri,
        pafa.award_description AS award_description,
        pafa.record_type AS record_type,
        pafa.awarding_agency_code AS awarding_agency_code,
        pafa.awarding_agency_name AS awarding_agency_name,
        pafa.awarding_office_code AS awarding_office_code,
        pafa.awarding_office_name AS awarding_office_name,
        pafa.funding_agency_code AS funding_agency_code,
        pafa.funding_agency_name AS funding_agency_name,
        pafa.funding_office_code AS funding_office_code,
        pafa.funding_office_name AS funding_office_name,
        pafa.business_types_desc AS business_types_desc,
        pafa.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
        pafa.awarding_sub_tier_agency_n AS awarding_sub_tier_agency_n,
        pafa.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
        pafa.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
        pafa.unique_award_key AS unique_award_key
    FROM published_award_financial_assistance AS pafa
    WHERE is_active IS TRUE
        AND EXISTS (
            SELECT 1
            FROM fsrs_grant
            WHERE fsrs_grant.fain = pafa.fain
        )
    ORDER BY pafa.fain, pafa.action_date),
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
UPDATE subaward
SET
    -- File F Prime Awards
    unique_award_key = aw_pafa.unique_award_key,
    award_id = fsrs_grant.fain,
    parent_award_id = NULL,
    award_amount = fsrs_grant.total_fed_funding_amount,
    action_date = fsrs_grant.obligation_date,
    fy = 'FY' || fy(obligation_date),
    awarding_agency_code = aw_pafa.awarding_agency_code,
    awarding_agency_name = aw_pafa.awarding_agency_name,
    awarding_sub_tier_agency_c = fsrs_grant.federal_agency_id,
    awarding_sub_tier_agency_n = aw_pafa.awarding_sub_tier_agency_n,
    awarding_office_code = aw_pafa.awarding_office_code,
    awarding_office_name = aw_pafa.awarding_office_name,
    funding_agency_code = aw_pafa.funding_agency_code,
    funding_agency_name = aw_pafa.funding_agency_name,
    funding_sub_tier_agency_co = aw_pafa.funding_sub_tier_agency_co,
    funding_sub_tier_agency_na = aw_pafa.funding_sub_tier_agency_na,
    funding_office_code = aw_pafa.funding_office_code,
    funding_office_name = aw_pafa.funding_office_name,
    awardee_or_recipient_uniqu = fsrs_grant.duns,
    awardee_or_recipient_legal = fsrs_grant.awardee_name,
    dba_name = fsrs_grant.dba_name,
    ultimate_parent_unique_ide = fsrs_grant.parent_duns,
    ultimate_parent_legal_enti = grant_pduns.legal_business_name,
    legal_entity_country_code = fsrs_grant.awardee_address_country,
    legal_entity_country_name = le_country.country_name,
    legal_entity_address_line1 = fsrs_grant.awardee_address_street,
    legal_entity_city_name = fsrs_grant.awardee_address_city,
    legal_entity_state_code = fsrs_grant.awardee_address_state,
    legal_entity_state_name = fsrs_grant.awardee_address_state_name,
    legal_entity_zip = CASE WHEN fsrs_grant.awardee_address_country = 'USA'
                            THEN fsrs_grant.awardee_address_zip
                            ELSE NULL
                       END,
    legal_entity_congressional = fsrs_grant.awardee_address_district,
    legal_entity_foreign_posta = CASE WHEN fsrs_grant.awardee_address_country <> 'USA'
                                      THEN fsrs_grant.awardee_address_zip
                                      ELSE NULL
                                 END,
    business_types = aw_pafa.business_types_desc,
    place_of_perform_city_name = fsrs_grant.principle_place_city,
    place_of_perform_state_code = fsrs_grant.principle_place_state,
    place_of_perform_state_name = fsrs_grant.principle_place_state_name,
    place_of_performance_zip = fsrs_grant.principle_place_zip,
    place_of_perform_congressio = fsrs_grant.principle_place_district,
    place_of_perform_country_co = fsrs_grant.principle_place_country,
    place_of_perform_country_na = ppop_country.country_name,
    award_description = fsrs_grant.project_description,
    naics = NULL,
    naics_description = NULL,
    cfda_numbers = CASE WHEN fsrs_grant.cfda_numbers ~ ';'
                        THEN cfda_num_loop(fsrs_grant.cfda_numbers)
                        ELSE cfda_num(fsrs_grant.cfda_numbers)
                   END,
    cfda_titles = CASE WHEN fsrs_grant.cfda_numbers ~ ';'
                       THEN cfda_word_loop(fsrs_grant.cfda_numbers)
                       ELSE cfda_word(fsrs_grant.cfda_numbers)
                  END,

    -- File F Subawards
    subaward_type = 'sub-grant',
    subaward_report_year = fsrs_grant.report_period_year,
    subaward_report_month = fsrs_grant.report_period_mon,
    subaward_number = fsrs_subgrant.subaward_num,
    subaward_amount = fsrs_subgrant.subaward_amount,
    sub_action_date = fsrs_subgrant.subaward_date,
    sub_awardee_or_recipient_uniqu = fsrs_subgrant.duns,
    sub_awardee_or_recipient_legal = fsrs_subgrant.awardee_name,
    sub_dba_name = fsrs_subgrant.dba_name,
    sub_ultimate_parent_unique_ide = fsrs_subgrant.parent_duns,
    sub_ultimate_parent_legal_enti = subgrant_pduns.legal_business_name,
    sub_legal_entity_country_code = fsrs_subgrant.awardee_address_country,
    sub_legal_entity_country_name = sub_le_country.country_name,
    sub_legal_entity_address_line1 = fsrs_subgrant.awardee_address_street,
    sub_legal_entity_city_name = fsrs_subgrant.awardee_address_city,
    sub_legal_entity_state_code = fsrs_subgrant.awardee_address_state,
    sub_legal_entity_state_name = fsrs_subgrant.awardee_address_state_name,
    sub_legal_entity_zip = CASE WHEN fsrs_subgrant.awardee_address_country = 'USA'
                                THEN fsrs_subgrant.awardee_address_zip
                                ELSE NULL
                           END,
    sub_legal_entity_congressional = fsrs_subgrant.awardee_address_district,
    sub_legal_entity_foreign_posta = CASE WHEN fsrs_subgrant.awardee_address_country <> 'USA'
                                          THEN fsrs_subgrant.awardee_address_zip
                                          ELSE NULL
                                     END,
    sub_business_types = array_to_string(subgrant_duns.business_types_codes, ', '),
    sub_place_of_perform_city_name = fsrs_subgrant.principle_place_city,
    sub_place_of_perform_state_code = fsrs_subgrant.principle_place_state,
    sub_place_of_perform_state_name = fsrs_subgrant.principle_place_state_name,
    sub_place_of_performance_zip = fsrs_subgrant.principle_place_zip,
    sub_place_of_perform_congressio = fsrs_subgrant.principle_place_district,
    sub_place_of_perform_country_co = fsrs_subgrant.principle_place_country,
    sub_place_of_perform_country_na = sub_ppop_country.country_name,
    subaward_description = fsrs_subgrant.project_description,
    sub_high_comp_officer1_full_na = fsrs_subgrant.top_paid_fullname_1,
    sub_high_comp_officer1_amount = fsrs_subgrant.top_paid_amount_1,
    sub_high_comp_officer2_full_na = fsrs_subgrant.top_paid_fullname_2,
    sub_high_comp_officer2_amount = fsrs_subgrant.top_paid_amount_2,
    sub_high_comp_officer3_full_na = fsrs_subgrant.top_paid_fullname_3,
    sub_high_comp_officer3_amount = fsrs_subgrant.top_paid_amount_3,
    sub_high_comp_officer4_full_na = fsrs_subgrant.top_paid_fullname_4,
    sub_high_comp_officer4_amount = fsrs_subgrant.top_paid_amount_4,
    sub_high_comp_officer5_full_na = fsrs_subgrant.top_paid_fullname_5,
    sub_high_comp_officer5_amount = fsrs_subgrant.top_paid_amount_5,

    -- File F Prime Awards
    prime_id = fsrs_grant.id,
    internal_id = fsrs_grant.internal_id,
    date_submitted = fsrs_grant.date_submitted,
    report_type = NULL,
    transaction_type = NULL,
    program_title = NULL,
    contract_agency_code = NULL,
    contract_idv_agency_code = NULL,
    grant_funding_agency_id = fsrs_grant.funding_agency_id,
    grant_funding_agency_name = fsrs_grant.funding_agency_name,
    federal_agency_name = fsrs_grant.federal_agency_name,
    treasury_symbol = NULL,
    dunsplus4 = fsrs_grant.dunsplus4,
    recovery_model_q1 = NULL,
    recovery_model_q2 = NULL,
    compensation_q1 = fsrs_grant.compensation_q1,
    compensation_q2 = fsrs_grant.compensation_q2,
    high_comp_officer1_full_na = fsrs_grant.top_paid_fullname_1,
    high_comp_officer1_amount = fsrs_grant.top_paid_amount_1,
    high_comp_officer2_full_na = fsrs_grant.top_paid_fullname_2,
    high_comp_officer2_amount = fsrs_grant.top_paid_amount_2,
    high_comp_officer3_full_na = fsrs_grant.top_paid_fullname_3,
    high_comp_officer3_amount = fsrs_grant.top_paid_amount_3,
    high_comp_officer4_full_na = fsrs_grant.top_paid_fullname_4,
    high_comp_officer4_amount = fsrs_grant.top_paid_amount_4,
    high_comp_officer5_full_na = fsrs_grant.top_paid_fullname_5,
    high_comp_officer5_amount = fsrs_grant.top_paid_amount_5,

    -- File F Subawards
    sub_id = fsrs_subgrant.id,
    sub_parent_id = fsrs_subgrant.parent_id,
    sub_federal_agency_id = fsrs_subgrant.federal_agency_id,
    sub_federal_agency_name = fsrs_subgrant.federal_agency_name,
    sub_funding_agency_id = fsrs_subgrant.funding_agency_id,
    sub_funding_agency_name = fsrs_subgrant.funding_agency_name,
    sub_funding_office_id = NULL,
    sub_funding_office_name = NULL,
    sub_naics = NULL,
    sub_cfda_numbers = fsrs_subgrant.cfda_numbers,
    sub_dunsplus4 = fsrs_subgrant.dunsplus4,
    sub_recovery_subcontract_amt = NULL,
    sub_recovery_model_q1 = NULL,
    sub_recovery_model_q2 = NULL,
    sub_compensation_q1 = fsrs_subgrant.compensation_q1,
    sub_compensation_q2 = fsrs_subgrant.compensation_q2,
    "updated_at" = NOW()

FROM fsrs_grant
    JOIN fsrs_subgrant
        ON fsrs_subgrant.parent_id = fsrs_grant.id
    LEFT OUTER JOIN aw_pafa
        ON fsrs_grant.fain = aw_pafa.fain
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
WHERE subaward.unique_award_key IS NULL
    AND subaward.subaward_type = 'sub-grant'
    AND subaward.prime_id = fsrs_grant.id
