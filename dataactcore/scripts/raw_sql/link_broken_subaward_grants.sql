CREATE TEMPORARY TABLE ON COMMIT DROP unlinked_subs AS
    (
        SELECT id,
            prime_id,
            sub_id,
            award_id,
            awarding_sub_tier_agency_c
        FROM subaward
        WHERE subaward.unique_award_key IS NULL
            AND subaward.subaward_type = 'sub-grant'
    );

CREATE TEMPORARY TABLE ON COMMIT DROP aw_pf AS
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
        COALESCE(pf.legal_entity_zip5, '') || COALESCE(pf.legal_entity_zip_last4, '') AS legal_entity_zip,
        pf.legal_entity_congressional AS legal_entity_congressional,
        pf.legal_entity_foreign_posta AS legal_entity_foreign_posta,
        pf.place_of_performance_city AS place_of_performance_city,
        pf.place_of_perfor_state_code AS place_of_perfor_state_code,
        pf.place_of_perform_state_nam AS place_of_perform_state_nam,
        TRANSLATE(pf.place_of_performance_zip4a, '-', '') AS place_of_performance_zip,
        pf.place_of_performance_congr AS place_of_performance_congr,
        pf.action_date AS action_date,
        pf.cfda_number AS cfda_number,
        pf.cfda_title AS cfda_title,
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
            FROM unlinked_subs
            WHERE record_type = 2
                AND UPPER(TRANSLATE(unlinked_subs.award_id, '-', '')) = UPPER(TRANSLATE(pf.fain, '-', ''))
                AND UPPER(unlinked_subs.awarding_sub_tier_agency_c) IS NOT DISTINCT FROM UPPER(pf.awarding_sub_tier_agency_c)
        )
        {0});
CREATE INDEX ix_aw_pf_fain_upp ON aw_pf (UPPER(fain));
CREATE INDEX ix_aw_pf_sub_upp ON aw_pf (UPPER(awarding_sub_tier_agency_c));
CREATE INDEX ix_aw_pf_act_date ON aw_pf (action_date);
CREATE INDEX ix_aw_pf_act_date_desc ON aw_pf (action_date DESC);
CREATE INDEX ix_aw_pf_act_type ON aw_pf (action_type_sort);
CREATE INDEX ix_aw_pf_act_type_desc ON aw_pf (action_type_sort DESC);
CREATE INDEX ix_aw_pf_mod_num_sort ON aw_pf (mod_num_sort);
CREATE INDEX ix_aw_pf_mod_num_sort_desc ON aw_pf (mod_num_sort DESC);

CREATE TEMPORARY TABLE base_aw_pf ON COMMIT DROP AS
    (SELECT DISTINCT ON (
            UPPER(pf.fain),
            UPPER(pf.awarding_sub_tier_agency_c)
        )
        pf.fain AS fain,
        pf.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
        cast_as_date(pf.action_date) AS action_date,
        pf.award_description
    FROM aw_pf AS pf
    ORDER BY UPPER(pf.fain), UPPER(pf.awarding_sub_tier_agency_c), pf.action_date, pf.action_type_sort, pf.mod_num_sort
    );
CREATE INDEX ix_base_aw_pf_fain_upp_trans ON base_aw_pf (UPPER(TRANSLATE(fain, '-', '')));

CREATE TEMPORARY TABLE latest_aw_pf ON COMMIT DROP AS
    (SELECT DISTINCT ON (
            UPPER(pf.fain),
            UPPER(pf.awarding_sub_tier_agency_c)
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
        pf.legal_entity_zip AS legal_entity_zip,
        pf.legal_entity_congressional AS legal_entity_congressional,
        pf.legal_entity_foreign_posta AS legal_entity_foreign_posta,
        pf.place_of_performance_city AS place_of_performance_city,
        pf.place_of_perfor_state_code AS place_of_perfor_state_code,
        pf.place_of_perform_state_nam AS place_of_perform_state_nam,
        pf.place_of_performance_zip AS place_of_performance_zip,
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
    ORDER BY UPPER(pf.fain), UPPER(pf.awarding_sub_tier_agency_c), pf.action_date DESC, pf.action_type_sort DESC, pf.mod_num_sort DESC
    );
CREATE INDEX ix_latest_aw_pf_uei_upp ON latest_aw_pf (UPPER(uei));
CREATE INDEX ix_latest_aw_pf_fain_upp_trans ON latest_aw_pf (UPPER(TRANSLATE(fain, '-', '')));

CREATE TEMPORARY TABLE grouped_aw_pf ON COMMIT DROP AS
    (SELECT pf.fain,
        pf.awarding_sub_tier_agency_c,
        array_agg(DISTINCT pf.cfda_number) AS cfda_nums,
        array_agg(DISTINCT cfda.program_title) AS cfda_names,
        SUM(pf.federal_action_obligation) AS award_amount
     FROM aw_pf AS pf
     LEFT OUTER JOIN cfda_program AS cfda
        ON to_char(cfda.program_number, 'FM00.000') = pf.cfda_number
     GROUP BY fain, awarding_sub_tier_agency_c
     );
CREATE INDEX ix_grouped_aw_pf_fain_upp_trans ON grouped_aw_pf (UPPER(TRANSLATE(fain, '-', '')));

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
    award_amount = gap.award_amount,
    action_date = bap.action_date,
    fy = 'FY' || fy(bap.action_date),
    awarding_agency_code = lap.awarding_agency_code,
    awarding_agency_name = lap.awarding_agency_name,
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
    legal_entity_address_line1 = lap.legal_entity_address_line1,
    legal_entity_city_name = lap.legal_entity_city_name,
    legal_entity_state_code = lap.legal_entity_state_code,
    legal_entity_state_name = lap.legal_entity_state_name,
    legal_entity_zip = CASE WHEN lap.legal_entity_country_code = 'USA'
                            THEN lap.legal_entity_zip
                            ELSE NULL
                       END,
    legal_entity_congressional = lap.legal_entity_congressional,
    legal_entity_foreign_posta = CASE WHEN lap.legal_entity_country_code <> 'USA'
                                      THEN lap.legal_entity_foreign_posta
                                      ELSE NULL
                                  END,
    business_types = lap.business_types_desc,
    place_of_perform_city_name = lap.place_of_performance_city,
    place_of_perform_state_code = lap.place_of_perfor_state_code,
    place_of_perform_state_name = lap.place_of_perform_state_nam,
    place_of_performance_zip = lap.place_of_performance_zip,
    place_of_perform_congressio = lap.place_of_performance_congr,
    award_description = bap.award_description,
    cfda_numbers = ARRAY_TO_STRING(gap.cfda_nums, ', '),
    cfda_titles = ARRAY_TO_STRING(gap.cfda_names, ', '),
    high_comp_officer1_full_na = lap.high_comp_officer1_full_na,
    high_comp_officer1_amount = lap.high_comp_officer1_amount,
    high_comp_officer2_full_na = lap.high_comp_officer2_full_na,
    high_comp_officer2_amount = lap.high_comp_officer2_amount,
    high_comp_officer3_full_na = lap.high_comp_officer3_full_na,
    high_comp_officer3_amount = lap.high_comp_officer3_amount,
    high_comp_officer4_full_na = lap.high_comp_officer4_full_na,
    high_comp_officer4_amount = lap.high_comp_officer4_amount,
    high_comp_officer5_full_na = lap.high_comp_officer5_full_na,
    high_comp_officer5_amount = lap.high_comp_officer5_amount
FROM unlinked_subs
     JOIN latest_aw_pf AS lap
        ON UPPER(TRANSLATE(unlinked_subs.award_id, '-', '')) = UPPER(TRANSLATE(lap.fain, '-', ''))
        AND UPPER(unlinked_subs.awarding_sub_tier_agency_c) IS NOT DISTINCT FROM UPPER(lap.awarding_sub_tier_agency_c)
    JOIN base_aw_pf AS bap
        ON UPPER(TRANSLATE(unlinked_subs.award_id, '-', '')) = UPPER(TRANSLATE(bap.fain, '-', ''))
        AND UPPER(unlinked_subs.awarding_sub_tier_agency_c) IS NOT DISTINCT FROM UPPER(bap.awarding_sub_tier_agency_c)
    JOIN grouped_aw_pf AS gap
        ON UPPER(TRANSLATE(unlinked_subs.award_id, '-', '')) = UPPER(TRANSLATE(gap.fain, '-', ''))
        AND UPPER(unlinked_subs.awarding_sub_tier_agency_c) IS NOT DISTINCT FROM UPPER(gap.awarding_sub_tier_agency_c)
    LEFT OUTER JOIN grant_uei
        ON UPPER(lap.uei) = UPPER(grant_uei.uei)
WHERE subaward.id = unlinked_subs.id;
