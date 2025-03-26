CREATE TEMPORARY TABLE unlinked_subs ON COMMIT DROP AS
    (SELECT id,
            internal_id
        FROM subaward
        WHERE subaward.unique_award_key IS NULL
            AND subaward.subaward_type = 'sub-grant'
    );

CREATE TEMPORARY TABLE related_raw ON COMMIT DROP AS
    (SELECT UPPER(sam_subgrant.unique_award_key) AS unique_award_key,
            unlinked_subs.internal_id,
            unlinked_subs.id
     FROM sam_subgrant
     JOIN unlinked_subs
        ON unlinked_subs.internal_id = sam_subgrant.subaward_report_number
    );

CREATE TEMPORARY TABLE aw_pf ON COMMIT DROP AS
    (SELECT pf.fain AS fain,
        pf.award_description AS award_description,
        pf.record_type AS record_type,
        pf.awarding_agency_code AS awarding_agency_code,
        pf.awarding_agency_name AS awarding_agency_name,
        pf.awarding_office_code AS awarding_office_code,
        pf.awarding_office_name AS awarding_office_name,
        pf.funding_agency_code AS funding_agency_code,
        pf.funding_agency_name AS funding_agency_name,
        pf.funding_office_code AS funding_office_code,
        pf.funding_office_name AS funding_office_name,
        pf.business_types_desc AS business_types_desc,
        pf.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
        pf.awarding_sub_tier_agency_n AS awarding_sub_tier_agency_n,
        pf.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
        pf.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
        pf.unique_award_key AS unique_award_key,
        pf.awardee_or_recipient_uniqu AS awardee_or_recipient_uniqu,
        pf.ultimate_parent_unique_ide AS ultimate_parent_unique_ide,
        pf.uei AS uei,
        pf.ultimate_parent_uei AS ultimate_parent_uei,
        pf.awardee_or_recipient_legal AS awardee_or_recipient_legal,
        pf.ultimate_parent_legal_enti AS ultimate_parent_legal_enti,
        pf.legal_entity_address_line1 AS legal_entity_address_line1,
        pf.legal_entity_city_name AS legal_entity_city_name,
        pf.legal_entity_state_code AS legal_entity_state_code,
        pf.legal_entity_state_name AS legal_entity_state_name,
        UPPER(pf.legal_entity_country_code) AS legal_entity_country_code,
        pf.legal_entity_country_name AS legal_entity_country_name,
        COALESCE(pf.legal_entity_zip5, '') || COALESCE(pf.legal_entity_zip_last4, '') AS legal_entity_zip,
        pf.legal_entity_county_code AS legal_entity_county_code,
        pf.legal_entity_county_name AS legal_entity_county_name,
        pf.legal_entity_congressional AS legal_entity_congressional,
        pf.legal_entity_foreign_posta AS legal_entity_foreign_posta,
        UPPER(pf.place_of_perform_country_c) AS place_of_perform_country_c,
        pf.place_of_perform_country_n AS place_of_perform_country_n,
        pf.place_of_performance_city AS place_of_performance_city,
        pf.place_of_perfor_state_code AS place_of_perfor_state_code,
        pf.place_of_perform_state_nam AS place_of_perform_state_nam,
        TRANSLATE(pf.place_of_performance_zip4a, '-', '') AS place_of_performance_zip,
        pf.place_of_perform_county_co AS place_of_perform_county_co,
        pf.place_of_perform_county_na AS place_of_perform_county_na,
        pf.place_of_performance_congr AS place_of_performance_congr,
        pf.action_date AS action_date,
        pf.assistance_listing_number AS assistance_listing_number,
        pf.assistance_listing_title AS assistance_listing_title,
        pf.federal_action_obligation AS federal_action_obligation,
        pf.high_comp_officer1_full_na AS high_comp_officer1_full_na,
        pf.high_comp_officer1_amount AS high_comp_officer1_amount,
        pf.high_comp_officer2_full_na AS high_comp_officer2_full_na,
        pf.high_comp_officer2_amount AS high_comp_officer2_amount,
        pf.high_comp_officer3_full_na AS high_comp_officer3_full_na,
        pf.high_comp_officer3_amount AS high_comp_officer3_amount,
        pf.high_comp_officer4_full_na AS high_comp_officer4_full_na,
        pf.high_comp_officer4_amount AS high_comp_officer4_amount,
        pf.high_comp_officer5_full_na AS high_comp_officer5_full_na,
        pf.high_comp_officer5_amount AS high_comp_officer5_amount,
        -- The following are used for sorting by base vs latest transaction to put them in the proper order
        CASE WHEN UPPER(action_type) = 'A' THEN 1
            WHEN UPPER(action_type) = 'E' THEN 2
            ELSE 3
        END AS action_type_sort,
        CASE WHEN COALESCE(award_modification_amendme, '') = '' THEN '0'
            ELSE award_modification_amendme
        END AS mod_num_sort
    FROM published_fabs AS pf
    WHERE is_active IS TRUE
        AND EXISTS (
            SELECT 1
            FROM related_raw
            WHERE related_raw.unique_award_key = UPPER(pf.unique_award_key)
        ));
CREATE INDEX ix_aw_pf_fain_upp ON aw_pf (unique_award_key);
CREATE INDEX ix_aw_pf_act_date ON aw_pf (action_date);
CREATE INDEX ix_aw_pf_act_date_desc ON aw_pf (action_date DESC);
CREATE INDEX ix_aw_pf_act_type ON aw_pf (action_type_sort);
CREATE INDEX ix_aw_pf_act_type_desc ON aw_pf (action_type_sort DESC);
CREATE INDEX ix_aw_pf_mod_num_sort ON aw_pf (mod_num_sort);
CREATE INDEX ix_aw_pf_mod_num_sort_desc ON aw_pf (mod_num_sort DESC);

CREATE TEMPORARY TABLE base_aw_pf ON COMMIT DROP AS
    (SELECT DISTINCT ON (
            pf.unique_award_key
        )
        pf.unique_award_key AS unique_award_key,
        cast_as_date(pf.action_date) AS action_date,
        pf.award_description
    FROM aw_pf AS pf
    ORDER BY pf.unique_award_key, pf.action_date, pf.action_type_sort, pf.mod_num_sort
    );
CREATE INDEX ix_base_aw_pf_uak ON base_aw_pf (unique_award_key);

CREATE TEMPORARY TABLE latest_aw_pf ON COMMIT DROP AS
   (SELECT DISTINCT ON (
            pf.unique_award_key
        )
        pf.fain AS fain,
        pf.record_type AS record_type,
        pf.awarding_agency_code AS awarding_agency_code,
        pf.awarding_agency_name AS awarding_agency_name,
        pf.awarding_office_code AS awarding_office_code,
        pf.awarding_office_name AS awarding_office_name,
        pf.funding_agency_code AS funding_agency_code,
        pf.funding_agency_name AS funding_agency_name,
        pf.funding_office_code AS funding_office_code,
        pf.funding_office_name AS funding_office_name,
        pf.business_types_desc AS business_types_desc,
        pf.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
        pf.awarding_sub_tier_agency_n AS awarding_sub_tier_agency_n,
        pf.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
        pf.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
        pf.unique_award_key AS unique_award_key,
        pf.awardee_or_recipient_uniqu AS awardee_or_recipient_uniqu,
        pf.ultimate_parent_unique_ide AS ultimate_parent_unique_ide,
        pf.uei AS uei,
        pf.ultimate_parent_uei AS ultimate_parent_uei,
        pf.awardee_or_recipient_legal AS awardee_or_recipient_legal,
        pf.ultimate_parent_legal_enti AS ultimate_parent_legal_enti,
        pf.legal_entity_address_line1 AS legal_entity_address_line1,
        pf.legal_entity_city_name AS legal_entity_city_name,
        pf.legal_entity_state_code AS legal_entity_state_code,
        pf.legal_entity_state_name AS legal_entity_state_name,
        pf.legal_entity_country_code AS legal_entity_country_code,
        pf.legal_entity_country_name AS legal_entity_country_name,
        pf.legal_entity_zip AS legal_entity_zip,
        pf.legal_entity_county_code AS legal_entity_county_code,
        pf.legal_entity_county_name AS legal_entity_county_name,
        pf.legal_entity_congressional AS legal_entity_congressional,
        pf.legal_entity_foreign_posta AS legal_entity_foreign_posta,
        pf.place_of_perform_country_c AS place_of_perform_country_c,
        pf.place_of_perform_country_n AS place_of_perform_country_n,
        pf.place_of_performance_city AS place_of_performance_city,
        pf.place_of_perfor_state_code AS place_of_perfor_state_code,
        pf.place_of_perform_state_nam AS place_of_perform_state_nam,
        pf.place_of_performance_zip AS place_of_performance_zip,
        pf.place_of_perform_county_co AS place_of_perform_county_co,
        pf.place_of_perform_county_na AS place_of_perform_county_na,
        pf.place_of_performance_congr AS place_of_performance_congr,
        pf.high_comp_officer1_full_na AS high_comp_officer1_full_na,
        pf.high_comp_officer1_amount AS high_comp_officer1_amount,
        pf.high_comp_officer2_full_na AS high_comp_officer2_full_na,
        pf.high_comp_officer2_amount AS high_comp_officer2_amount,
        pf.high_comp_officer3_full_na AS high_comp_officer3_full_na,
        pf.high_comp_officer3_amount AS high_comp_officer3_amount,
        pf.high_comp_officer4_full_na AS high_comp_officer4_full_na,
        pf.high_comp_officer4_amount AS high_comp_officer4_amount,
        pf.high_comp_officer5_full_na AS high_comp_officer5_full_na,
        pf.high_comp_officer5_amount AS high_comp_officer5_amount
    FROM aw_pf AS pf
    ORDER BY pf.unique_award_key, pf.action_date DESC, pf.action_type_sort DESC, pf.mod_num_sort DESC
    );
CREATE INDEX ix_latest_aw_pf_uei_upp ON latest_aw_pf (UPPER(uei));
CREATE INDEX ix_latest_aw_pf_uak ON latest_aw_pf (unique_award_key);

CREATE TEMPORARY TABLE grouped_aw_pf ON COMMIT DROP AS
    (SELECT pf.unique_award_key,
        array_agg(DISTINCT pf.assistance_listing_number) AS assistance_listing_nums,
        array_agg(DISTINCT al.program_title) AS assistance_listing_names,
        SUM(pf.federal_action_obligation) AS award_amount
     FROM aw_pf AS pf
     LEFT OUTER JOIN assistance_listing AS al
        ON to_char(al.program_number, 'FM00.000') = pf.assistance_listing_number
     GROUP BY unique_award_key
     );
CREATE INDEX ix_grouped_aw_pf_uak ON grouped_aw_pf (unique_award_key);

CREATE TEMPORARY TABLE grant_uei ON COMMIT DROP AS
    (SELECT grant_uei_from.uei AS uei,
        grant_uei_from.legal_business_name AS legal_business_name,
        grant_uei_from.dba_name AS dba_name
    FROM (
        SELECT sam_recipient.uei AS uei,
            sam_recipient.legal_business_name AS legal_business_name,
            sam_recipient.dba_name AS dba_name,
            row_number() OVER (PARTITION BY
                UPPER(sam_recipient.uei)
            ) AS row
        FROM latest_aw_pf
            LEFT OUTER JOIN sam_recipient
                ON UPPER(latest_aw_pf.uei) = UPPER(sam_recipient.uei)
        ORDER BY sam_recipient.activation_date DESC
     ) AS grant_uei_from
    WHERE grant_uei_from.row = 1);
CREATE INDEX ix_grant_uei_upp ON grant_uei (UPPER(uei));

UPDATE subaward
SET
    unique_award_key = lap.unique_award_key,
    award_id = lap.fain,
    award_amount = gap.award_amount,
    action_date = bap.action_date,
    fy = 'FY' || fy(bap.action_date),
    awarding_agency_code = lap.awarding_agency_code,
    awarding_agency_name = lap.awarding_agency_name,
    awarding_sub_tier_agency_c = lap.awarding_sub_tier_agency_c,
    awarding_sub_tier_agency_n = lap.awarding_sub_tier_agency_n,
    awarding_office_code = lap.awarding_office_code,
    awarding_office_name = lap.awarding_office_name,
    funding_agency_code = lap.funding_agency_code,
    funding_agency_name = lap.funding_agency_name,
    funding_sub_tier_agency_co = lap.funding_sub_tier_agency_co,
    funding_sub_tier_agency_na = lap.funding_sub_tier_agency_na,
    funding_office_code = lap.funding_office_code,
    funding_office_name = lap.funding_office_name,
    awardee_or_recipient_uniqu = lap.awardee_or_recipient_uniqu,
    awardee_or_recipient_uei = lap.uei,
    awardee_or_recipient_legal = lap.awardee_or_recipient_legal,
    dba_name = grant_uei.dba_name,
    ultimate_parent_unique_ide = lap.ultimate_parent_unique_ide,
    ultimate_parent_uei = lap.ultimate_parent_uei,
    ultimate_parent_legal_enti = lap.ultimate_parent_legal_enti,
    legal_entity_country_code = lap.legal_entity_country_code,
    legal_entity_country_name = lap.legal_entity_country_name,
    legal_entity_address_line1 = lap.legal_entity_address_line1,
    legal_entity_city_name = lap.legal_entity_city_name,
    legal_entity_state_code = lap.legal_entity_state_code,
    legal_entity_state_name = lap.legal_entity_state_name,
    legal_entity_zip = CASE WHEN lap.legal_entity_country_code = 'USA'
                            THEN lap.legal_entity_zip
                            ELSE NULL
                       END,
    legal_entity_county_code = lap.legal_entity_county_code,
    legal_entity_county_name = lap.legal_entity_county_name,
    legal_entity_congressional = lap.legal_entity_congressional,
    legal_entity_foreign_posta = CASE WHEN lap.legal_entity_country_code <> 'USA'
                                      THEN lap.legal_entity_foreign_posta
                                      ELSE NULL
                                  END,
    business_types = lap.business_types_desc,
    place_of_perform_country_co = lap.place_of_perform_country_c,
    place_of_perform_country_na = lap.place_of_perform_country_n,
    place_of_perform_city_name = lap.place_of_performance_city,
    place_of_perform_state_code = lap.place_of_perfor_state_code,
    place_of_perform_state_name = lap.place_of_perform_state_nam,
    place_of_performance_zip = lap.place_of_performance_zip,
    place_of_performance_county_code = lap.place_of_perform_county_co,
    place_of_performance_county_name = lap.place_of_perform_county_na,
    place_of_perform_congressio = lap.place_of_performance_congr,
    award_description = bap.award_description,
    assistance_listing_numbers = ARRAY_TO_STRING(gap.assistance_listing_nums, ', '),
    assistance_listing_titles = ARRAY_TO_STRING(gap.assistance_listing_names, ', '),
    grant_funding_agency_id = lap.funding_sub_tier_agency_co,
    grant_funding_agency_name = lap.funding_sub_tier_agency_na,
    federal_agency_name = lap.awarding_sub_tier_agency_c,
    high_comp_officer1_full_na = lap.high_comp_officer1_full_na,
    high_comp_officer1_amount = lap.high_comp_officer1_amount,
    high_comp_officer2_full_na = lap.high_comp_officer2_full_na,
    high_comp_officer2_amount = lap.high_comp_officer2_amount,
    high_comp_officer3_full_na = lap.high_comp_officer3_full_na,
    high_comp_officer3_amount = lap.high_comp_officer3_amount,
    high_comp_officer4_full_na = lap.high_comp_officer4_full_na,
    high_comp_officer4_amount = lap.high_comp_officer4_amount,
    high_comp_officer5_full_na = lap.high_comp_officer5_full_na,
    high_comp_officer5_amount = lap.high_comp_officer5_amount,

    -- Subaward values derived from prime award
    sub_assistance_listing_numbers = ARRAY_TO_STRING(gap.assistance_listing_nums, ', '),
    sub_federal_agency_id = lap.awarding_sub_tier_agency_c,
    sub_federal_agency_name = lap.awarding_sub_tier_agency_n,
    sub_funding_agency_id = lap.funding_sub_tier_agency_co,
    sub_funding_agency_name = lap.funding_sub_tier_agency_na
FROM related_raw
     JOIN latest_aw_pf AS lap
        ON related_raw.unique_award_key = lap.unique_award_key
    JOIN base_aw_pf AS bap
        ON related_raw.unique_award_key = bap.unique_award_key
    JOIN grouped_aw_pf AS gap
        ON related_raw.unique_award_key = gap.unique_award_key
    LEFT OUTER JOIN grant_uei
        ON UPPER(lap.uei) = UPPER(grant_uei.uei)
WHERE subaward.id = related_raw.id;
