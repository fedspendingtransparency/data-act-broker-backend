CREATE TEMPORARY TABLE unlinked_subs ON COMMIT DROP AS
    (
        SELECT id,
            internal_id
        FROM subaward
        WHERE subaward.unique_award_key IS NULL
            AND subaward.subaward_type = 'sub-contract'
    );

CREATE TEMPORARY TABLE related_raw ON COMMIT DROP AS
    (SELECT UPPER(sam_subcontract.unique_award_key) AS unique_award_key,
            unlinked_subs.internal_id,
            unlinked_subs.id
     FROM sam_subcontract
     JOIN unlinked_subs
        ON unlinked_subs.internal_id = sam_subcontract.subaward_report_number
    );

CREATE TEMPORARY TABLE aw_dap ON COMMIT DROP AS
    (SELECT UPPER(dap.unique_award_key) AS unique_award_key,
        dap.piid AS piid,
        dap.idv_type AS idv_type,
        dap.parent_award_id AS parent_award_id,
        dap.award_description as award_description,
        dap.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
        dap.awarding_sub_tier_agency_n AS awarding_sub_tier_agency_n,
        dap.naics AS naics,
        dap.naics_description AS naics_description,
        dap.awarding_agency_code AS awarding_agency_code,
        dap.awarding_agency_name AS awarding_agency_name,
        dap.funding_agency_code AS funding_agency_code,
        dap.funding_agency_name AS funding_agency_name,
        dap.awarding_office_code AS awarding_office_code,
        dap.awarding_office_name AS awarding_office_name,
        dap.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
        dap.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
        dap.funding_office_code AS funding_office_code,
        dap.funding_office_name AS funding_office_name,
        dap.awardee_or_recipient_uniqu AS awardee_or_recipient_uniqu,
        dap.awardee_or_recipient_uei AS awardee_or_recipient_uei,
        dap.awardee_or_recipient_legal AS awardee_or_recipient_legal,
        dap.ultimate_parent_unique_ide AS ultimate_parent_unique_ide,
        dap.ultimate_parent_uei AS ultimate_parent_uei,
        dap.ultimate_parent_legal_enti AS ultimate_parent_legal_enti,
        dap.legal_entity_address_line1 AS legal_entity_address_line1,
        dap.legal_entity_city_name AS legal_entity_city_name,
        dap.legal_entity_state_code AS legal_entity_state_code,
        dap.legal_entity_state_descrip AS legal_entity_state_descrip,
        dap.legal_entity_country_code AS legal_entity_country_code,
        dap.legal_entity_country_name AS legal_entity_country_name,
        dap.legal_entity_zip4 AS legal_entity_zip4,
        dap.legal_entity_county_code AS legal_entity_county_code,
        dap.legal_entity_county_name AS legal_entity_county_name,
        dap.legal_entity_congressional AS legal_entity_congressional,
        dap.place_of_perform_city_name AS place_of_perform_city_name,
        dap.place_of_performance_state AS place_of_performance_state,
        dap.place_of_perfor_state_desc AS place_of_perfor_state_desc,
        dap.place_of_performance_zip4a AS place_of_performance_zip4a,
        dap.place_of_perform_county_co AS place_of_perform_county_co,
        dap.place_of_perform_county_na AS place_of_perform_county_na,
        dap.place_of_performance_congr AS place_of_performance_congr,
        dap.place_of_perform_country_c AS place_of_perform_country_c,
        dap.place_of_perf_country_desc AS place_of_perf_country_desc,
        dap.high_comp_officer1_full_na AS high_comp_officer1_full_na,
        dap.high_comp_officer1_amount AS high_comp_officer1_amount,
        dap.high_comp_officer2_full_na AS high_comp_officer2_full_na,
        dap.high_comp_officer2_amount AS high_comp_officer2_amount,
        dap.high_comp_officer3_full_na AS high_comp_officer3_full_na,
        dap.high_comp_officer3_amount AS high_comp_officer3_amount,
        dap.high_comp_officer4_full_na AS high_comp_officer4_full_na,
        dap.high_comp_officer4_amount AS high_comp_officer4_amount,
        dap.high_comp_officer5_full_na AS high_comp_officer5_full_na,
        dap.high_comp_officer5_amount AS high_comp_officer5_amount,
        dap.total_obligated_amount AS total_obligated_amount,
        dap.vendor_doing_as_business_n AS vendor_doing_as_business_n,
        dap.action_date AS action_date,
        -- The following are used for sorting by base vs latest transaction to put them in the proper order
        CASE WHEN COALESCE(action_type, '') = '' THEN 1
            ELSE 2
        END AS action_type_sort,
        CASE WHEN COALESCE(award_modification_amendme, '') = '' THEN '0'
            ELSE award_modification_amendme
        END AS mod_num_sort
    FROM detached_award_procurement AS dap
    WHERE EXISTS (
        SELECT 1
        FROM related_raw
        WHERE related_raw.unique_award_key = UPPER(dap.unique_award_key)
    ));
CREATE INDEX ix_aw_dap_uak ON aw_dap (unique_award_key);
CREATE INDEX ix_aw_dap_act_date ON aw_dap (action_date);
CREATE INDEX ix_aw_dap_act_date_desc ON aw_dap (action_date DESC);
CREATE INDEX ix_aw_dap_act_type ON aw_dap (action_type_sort);
CREATE INDEX ix_aw_dap_act_type_desc ON aw_dap (action_type_sort DESC);
CREATE INDEX ix_aw_dap_mod_num_sort ON aw_dap (mod_num_sort);
CREATE INDEX ix_aw_dap_mod_num_sort_desc ON aw_dap (mod_num_sort DESC);

CREATE TEMPORARY TABLE base_aw_dap ON COMMIT DROP AS
    (SELECT DISTINCT ON (
            dap.unique_award_key
        )
        dap.unique_award_key AS unique_award_key,
        dap.idv_type AS idv_type,
        dap.award_description as award_description,
        cast_as_date(dap.action_date) AS action_date
    FROM aw_dap AS dap
    ORDER BY dap.unique_award_key, dap.action_date, dap.action_type_sort, dap.mod_num_sort
    );
CREATE INDEX ix_base_aw_dap_uak ON base_aw_dap (unique_award_key);

CREATE TEMPORARY TABLE latest_aw_dap ON COMMIT DROP AS
    (SELECT DISTINCT ON (
            dap.unique_award_key
        )
        dap.unique_award_key AS unique_award_key,
        dap.piid AS piid,
        dap.parent_award_id AS parent_award_id,
        dap.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
        dap.awarding_sub_tier_agency_n AS awarding_sub_tier_agency_n,
        dap.idv_type AS idv_type,
        dap.awarding_agency_code AS awarding_agency_code,
        dap.awarding_agency_name AS awarding_agency_name,
        dap.funding_agency_code AS funding_agency_code,
        dap.funding_agency_name AS funding_agency_name,
        dap.awarding_office_code AS awarding_office_code,
        dap.awarding_office_name AS awarding_office_name,
        dap.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
        dap.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
        dap.funding_office_code AS funding_office_code,
        dap.funding_office_name AS funding_office_name,
        dap.awardee_or_recipient_uniqu AS awardee_or_recipient_uniqu,
        dap.awardee_or_recipient_uei AS awardee_or_recipient_uei,
        dap.awardee_or_recipient_legal AS awardee_or_recipient_legal,
        dap.ultimate_parent_unique_ide AS ultimate_parent_unique_ide,
        dap.ultimate_parent_uei AS ultimate_parent_uei,
        dap.ultimate_parent_legal_enti AS ultimate_parent_legal_enti,
        dap.legal_entity_address_line1 AS legal_entity_address_line1,
        dap.legal_entity_city_name AS legal_entity_city_name,
        dap.legal_entity_state_code AS legal_entity_state_code,
        dap.legal_entity_state_descrip AS legal_entity_state_descrip,
        dap.legal_entity_country_code AS legal_entity_country_code,
        dap.legal_entity_country_name AS legal_entity_country_name,
        dap.legal_entity_zip4 AS legal_entity_zip4,
        dap.legal_entity_county_code AS legal_entity_county_code,
        dap.legal_entity_county_name AS legal_entity_county_name,
        dap.legal_entity_congressional AS legal_entity_congressional,
        dap.place_of_perform_city_name AS place_of_perform_city_name,
        dap.place_of_performance_state AS place_of_performance_state,
        dap.place_of_perfor_state_desc AS place_of_perfor_state_desc,
        dap.place_of_performance_zip4a AS place_of_performance_zip4a,
        dap.place_of_perform_county_co AS place_of_perform_county_co,
        dap.place_of_perform_county_na AS place_of_perform_county_na,
        dap.place_of_performance_congr AS place_of_performance_congr,
        dap.place_of_perform_country_c AS place_of_perform_country_c,
        dap.place_of_perf_country_desc AS place_of_perf_country_desc,
        dap.high_comp_officer1_full_na AS high_comp_officer1_full_na,
        dap.high_comp_officer1_amount AS high_comp_officer1_amount,
        dap.high_comp_officer2_full_na AS high_comp_officer2_full_na,
        dap.high_comp_officer2_amount AS high_comp_officer2_amount,
        dap.high_comp_officer3_full_na AS high_comp_officer3_full_na,
        dap.high_comp_officer3_amount AS high_comp_officer3_amount,
        dap.high_comp_officer4_full_na AS high_comp_officer4_full_na,
        dap.high_comp_officer4_amount AS high_comp_officer4_amount,
        dap.high_comp_officer5_full_na AS high_comp_officer5_full_na,
        dap.high_comp_officer5_amount AS high_comp_officer5_amount,
        dap.total_obligated_amount AS total_obligated_amount,
        dap.vendor_doing_as_business_n AS vendor_doing_as_business_n,
        dap.naics AS naics,
        dap.naics_description AS naics_description,
        cast_as_date(dap.action_date) AS action_date
    FROM aw_dap AS dap
    ORDER BY dap.unique_award_key, dap.action_date DESC, dap.action_type_sort DESC, dap.mod_num_sort DESC
    );
CREATE INDEX ix_latest_aw_dap_uak ON latest_aw_dap (unique_award_key);

-- Get sampled award recipient data
CREATE TEMPORARY TABLE prime_recipient ON COMMIT DROP AS (
	SELECT
	    uei,
	    business_types
	FROM sam_recipient AS sr
	WHERE EXISTS (
	    SELECT 1
	    FROM aw_dap
	    WHERE aw_dap.awardee_or_recipient_uei = sr.uei
	)
);
CREATE INDEX ix_pr_uei ON prime_recipient (uei);

UPDATE subaward
SET
    unique_award_key = ldap.unique_award_key,
    award_id = ldap.piid,
    parent_award_id = ldap.parent_award_id,
    award_amount = ldap.total_obligated_amount,
    action_date = bdap.action_date,
    fy = 'FY' || fy(bdap.action_date),
    awarding_agency_code = ldap.awarding_agency_code,
    awarding_agency_name = ldap.awarding_agency_name,
    awarding_sub_tier_agency_c = ldap.awarding_sub_tier_agency_c,
    awarding_sub_tier_agency_n = ldap.awarding_sub_tier_agency_n,
    awarding_office_code = ldap.awarding_office_code,
    awarding_office_name = ldap.awarding_office_name,
    funding_agency_code = ldap.funding_agency_code,
    funding_agency_name = ldap.funding_agency_name,
    funding_sub_tier_agency_co = ldap.funding_sub_tier_agency_co,
    funding_sub_tier_agency_na = ldap.funding_sub_tier_agency_na,
    funding_office_code = ldap.funding_office_code,
    funding_office_name = ldap.funding_office_name,
    awardee_or_recipient_uniqu = ldap.awardee_or_recipient_uniqu,
    awardee_or_recipient_uei = ldap.awardee_or_recipient_uei,
    awardee_or_recipient_legal = ldap.awardee_or_recipient_legal,
    dba_name = ldap.vendor_doing_as_business_n,
    ultimate_parent_unique_ide = ldap.ultimate_parent_unique_ide,
    ultimate_parent_uei = ldap.ultimate_parent_uei,
    ultimate_parent_legal_enti = ldap.ultimate_parent_legal_enti,
    legal_entity_country_code = ldap.legal_entity_country_code,
    legal_entity_country_name = ldap.legal_entity_country_name,
    legal_entity_address_line1 = ldap.legal_entity_address_line1,
    legal_entity_city_name = ldap.legal_entity_city_name,
    legal_entity_state_code = ldap.legal_entity_state_code,
    legal_entity_state_name = ldap.legal_entity_state_descrip,
    legal_entity_zip = CASE WHEN ldap.legal_entity_country_code = 'USA'
                            THEN ldap.legal_entity_zip4
                            ELSE NULL
                       END,
    legal_entity_county_code = ldap.legal_entity_county_code,
    legal_entity_county_name = ldap.legal_entity_county_name,
    legal_entity_congressional = ldap.legal_entity_congressional,
    legal_entity_foreign_posta = CASE WHEN ldap.legal_entity_country_code <> 'USA'
                                      THEN ldap.legal_entity_zip4
                                      ELSE NULL
                                 END,
    business_types = CASE WHEN cardinality(pr.business_types) > 0
     THEN array_to_string(pr.business_types, ',')
     ELSE NULL
    END,
    place_of_perform_city_name = ldap.place_of_perform_city_name,
    place_of_perform_state_code = ldap.place_of_performance_state,
    place_of_perform_state_name = ldap.place_of_perfor_state_desc,
    place_of_performance_zip = ldap.place_of_performance_zip4a,
    place_of_performance_county_code = ldap.place_of_perform_county_co,
    place_of_performance_county_name = ldap.place_of_perform_county_na,
    place_of_perform_congressio = ldap.place_of_performance_congr,
    place_of_perform_country_co = ldap.place_of_perform_country_c,
    place_of_perform_country_na = ldap.place_of_perf_country_desc,
    award_description = bdap.award_description,
    naics = ldap.naics,
    naics_description = ldap.naics_description,
    high_comp_officer1_full_na = ldap.high_comp_officer1_full_na,
    high_comp_officer1_amount = ldap.high_comp_officer1_amount,
    high_comp_officer2_full_na = ldap.high_comp_officer2_full_na,
    high_comp_officer2_amount = ldap.high_comp_officer2_amount,
    high_comp_officer3_full_na = ldap.high_comp_officer3_full_na,
    high_comp_officer3_amount = ldap.high_comp_officer3_amount,
    high_comp_officer4_full_na = ldap.high_comp_officer4_full_na,
    high_comp_officer4_amount = ldap.high_comp_officer4_amount,
    high_comp_officer5_full_na = ldap.high_comp_officer5_full_na,
    high_comp_officer5_amount = ldap.high_comp_officer5_amount,

    -- Subaward values derived from prime award
    sub_federal_agency_id = ldap.awarding_sub_tier_agency_c,
    sub_federal_agency_name = ldap.awarding_sub_tier_agency_n,
    sub_funding_agency_id = ldap.funding_sub_tier_agency_co,
    sub_funding_agency_name = ldap.funding_sub_tier_agency_na,
    sub_naics = ldap.naics
FROM related_raw
    JOIN base_aw_dap AS bdap
        ON related_raw.unique_award_key = bdap.unique_award_key
    JOIN latest_aw_dap AS ldap
        ON related_raw.unique_award_key = ldap.unique_award_key
    LEFT OUTER JOIN prime_recipient AS pr
        ON ldap.awardee_or_recipient_uei = pr.uei
WHERE subaward.id = related_raw.id;
