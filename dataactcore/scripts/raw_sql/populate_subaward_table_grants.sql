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
        COALESCE(pf.legal_entity_zip5, '') || COALESCE(pf.legal_entity_zip_last4, '') AS legal_entity_zip,
        pf.legal_entity_county_code AS legal_entity_county_code,
        pf.legal_entity_county_name AS legal_entity_county_name,
        pf.legal_entity_congressional AS legal_entity_congressional,
        pf.legal_entity_foreign_posta AS legal_entity_foreign_posta,
        pf.place_of_performance_city AS place_of_performance_city,
        pf.place_of_perfor_state_code AS place_of_perfor_state_code,
        pf.place_of_perform_state_nam AS place_of_perform_state_nam,
        TRANSLATE(pf.place_of_performance_zip4a, '-', '') AS place_of_performance_zip,
        pf.place_of_perform_county_co AS place_of_perform_county_co,
        pf.place_of_perform_county_na AS place_of_perform_county_na,
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
            FROM fsrs_grant
            WHERE record_type = 2
                AND fsrs_grant.id {0} {1}
                AND UPPER(TRANSLATE(fsrs_grant.fain, '-', '')) = UPPER(TRANSLATE(pf.fain, '-', ''))
                AND COALESCE(UPPER(fsrs_grant.federal_agency_id), '') = COALESCE(UPPER(pf.awarding_sub_tier_agency_c), '')
        )
    );
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
        pf.legal_entity_county_code AS legal_entity_county_code,
        pf.legal_entity_county_name AS legal_entity_county_name,
        pf.legal_entity_congressional AS legal_entity_congressional,
        pf.legal_entity_foreign_posta AS legal_entity_foreign_posta,
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
    WHERE grant_uei_from.row = 1
    );
CREATE INDEX ix_grant_uei_upp ON grant_uei (UPPER(uei));

CREATE TEMPORARY TABLE subgrant_puei ON COMMIT DROP AS (
    SELECT sub_puei_from.uei AS uei,
        sub_puei_from.legal_business_name AS legal_business_name
    FROM (
        SELECT sam_recipient.uei AS uei,
            sam_recipient.legal_business_name AS legal_business_name,
            row_number() OVER (PARTITION BY
                UPPER(sam_recipient.uei)
            ) AS row
        FROM fsrs_subgrant
            LEFT OUTER JOIN sam_recipient
                ON UPPER(fsrs_subgrant.parent_uei) = UPPER(sam_recipient.uei)
                AND fsrs_subgrant.parent_id {0} {1}
        ORDER BY sam_recipient.activation_date DESC
    ) AS sub_puei_from
    WHERE sub_puei_from.row = 1
    );
CREATE INDEX ix_subgrant_puei_upp ON subgrant_puei (UPPER(uei));

CREATE TEMPORARY TABLE subgrant_uei ON COMMIT DROP AS (
    SELECT sub_uei_from.uei AS uei,
        sub_uei_from.business_types AS business_types
    FROM (
        SELECT
            sam_recipient.uei AS uei,
            sam_recipient.business_types AS business_types,
            row_number() OVER (PARTITION BY
                UPPER(sam_recipient.uei)
            ) AS row
        FROM fsrs_subgrant
            LEFT OUTER JOIN sam_recipient
                ON UPPER(fsrs_subgrant.uei_number) = UPPER(sam_recipient.uei)
                AND fsrs_subgrant.parent_id {0} {1}
        ORDER BY sam_recipient.activation_date DESC
    ) AS sub_uei_from
    WHERE sub_uei_from.row = 1
    );
CREATE INDEX ix_subgrant_uei_upp ON subgrant_uei (UPPER(uei));

-- Getting a list of all the subaward zips we'll encounter to limit any massive joins
CREATE TEMPORARY TABLE all_sub_zips ON COMMIT DROP AS (
	SELECT DISTINCT awardee_address_zip AS "sub_zip"
	FROM fsrs_subgrant
	UNION
	SELECT DISTINCT principle_place_zip AS "sub_zip"
	FROM fsrs_subgrant
);
-- Matching on all the available zip9s
CREATE TEMPORARY TABLE all_sub_zip9s ON COMMIT DROP AS (
	SELECT sub_zip
	FROM all_sub_zips
	WHERE LENGTH(sub_zip) = 9
);
CREATE TEMPORARY TABLE modified_zips ON COMMIT DROP AS (
	SELECT CONCAT(zip5, zip_last4) AS "sub_zip", county_number
	FROM zips
	WHERE EXISTS (
		SELECT 1
		FROM all_sub_zip9s AS asz
		WHERE CONCAT(zip5, zip_last4) = asz.sub_zip
	)
);
-- Matching on all the available zip5 + states in zips_grouped (and any remaining zip9s not currently matched)
CREATE TEMPORARY TABLE all_sub_zip5s ON COMMIT DROP AS (
	SELECT sub_zip
	FROM all_sub_zips AS asz
	WHERE LENGTH(sub_zip) = 5
	UNION
	(SELECT sub_zip
	FROM all_sub_zip9s AS asz
	EXCEPT
	SELECT sub_zip
	FROM modified_zips AS mz)
);
CREATE INDEX ix_asz5s_l5 ON all_sub_zip5s (LEFT(sub_zip, 5));

-- Since counties can vary between a zip5 + state, we want to only match on when there's only one county and not guess
CREATE TEMPORARY TABLE single_zips_grouped ON COMMIT DROP AS (
	SELECT zip5, state_abbreviation
	FROM zips_grouped
	GROUP BY zip5, state_abbreviation
	HAVING COUNT(*) = 1
);
CREATE TEMPORARY TABLE zips_grouped_modified ON COMMIT DROP AS (
	SELECT zip5, state_abbreviation, county_number
	FROM zips_grouped AS zg
	WHERE EXISTS (
		SELECT 1
		FROM all_sub_zip5s AS asz
		WHERE zg.zip5 = LEFT(asz.sub_zip, 5)
	) AND EXISTS (
		SELECT 1
		FROM single_zips_grouped AS szg
		WHERE zg.zip5 = szg.zip5
			AND zg.state_abbreviation = szg.state_abbreviation
	)
);
-- Combine the two matching groups together and join later. make sure keep them separated with type to prevent dups
CREATE TEMPORARY TABLE zips_modified_union ON COMMIT DROP AS (
	SELECT sub_zip AS "sub_zip", NULL AS "state_abbreviation", county_number AS "county_number", 'zip9' AS "type"
	FROM modified_zips
	UNION
	SELECT zip5 AS "sub_zip", state_abbreviation AS "state_code", county_number AS "county_number", 'zip5+state' AS "type"
	FROM zips_grouped_modified
);

INSERT INTO subaward (
    "unique_award_key",
    "award_id",
    "parent_award_id",
    "award_amount",
    "action_date",
    "fy",
    "awarding_agency_code",
    "awarding_agency_name",
    "awarding_sub_tier_agency_c",
    "awarding_sub_tier_agency_n",
    "awarding_office_code",
    "awarding_office_name",
    "funding_agency_code",
    "funding_agency_name",
    "funding_sub_tier_agency_co",
    "funding_sub_tier_agency_na",
    "funding_office_code",
    "funding_office_name",
    "awardee_or_recipient_uniqu",
    "awardee_or_recipient_uei",
    "awardee_or_recipient_legal",
    "dba_name",
    "ultimate_parent_unique_ide",
    "ultimate_parent_uei",
    "ultimate_parent_legal_enti",
    "legal_entity_country_code",
    "legal_entity_country_name",
    "legal_entity_address_line1",
    "legal_entity_city_name",
    "legal_entity_state_code",
    "legal_entity_state_name",
    "legal_entity_zip",
    "legal_entity_county_code",
    "legal_entity_county_name",
    "legal_entity_congressional",
    "legal_entity_foreign_posta",
    "business_types",
    "place_of_perform_city_name",
    "place_of_perform_state_code",
    "place_of_perform_state_name",
    "place_of_performance_zip",
    "place_of_performance_county_code",
    "place_of_performance_county_name",
    "place_of_perform_congressio",
    "place_of_perform_country_co",
    "place_of_perform_country_na",
    "award_description",
    "naics",
    "naics_description",
    "cfda_numbers",
    "cfda_titles",
    "prime_id",
    "internal_id",
    "date_submitted",
    "report_type",
    "transaction_type",
    "program_title",
    "contract_agency_code",
    "contract_idv_agency_code",
    "grant_funding_agency_id",
    "grant_funding_agency_name",
    "federal_agency_name",
    "treasury_symbol",
    "dunsplus4",
    "recovery_model_q1",
    "recovery_model_q2",
    "compensation_q1",
    "compensation_q2",
    "high_comp_officer1_full_na",
    "high_comp_officer1_amount",
    "high_comp_officer2_full_na",
    "high_comp_officer2_amount",
    "high_comp_officer3_full_na",
    "high_comp_officer3_amount",
    "high_comp_officer4_full_na",
    "high_comp_officer4_amount",
    "high_comp_officer5_full_na",
    "high_comp_officer5_amount",
    "place_of_perform_street",
    "subaward_type",
    "subaward_report_year",
    "subaward_report_month",
    "subaward_number",
    "subaward_amount",
    "sub_action_date",
    "sub_awardee_or_recipient_uniqu",
    "sub_awardee_or_recipient_uei",
    "sub_awardee_or_recipient_legal",
    "sub_dba_name",
    "sub_ultimate_parent_unique_ide",
    "sub_ultimate_parent_uei",
    "sub_ultimate_parent_legal_enti",
    "sub_legal_entity_country_code",
    "sub_legal_entity_country_name",
    "sub_legal_entity_address_line1",
    "sub_legal_entity_city_name",
    "sub_legal_entity_state_code",
    "sub_legal_entity_state_name",
    "sub_legal_entity_zip",
    "sub_legal_entity_county_code",
    "sub_legal_entity_county_name",
    "sub_legal_entity_congressional",
    "sub_legal_entity_foreign_posta",
    "sub_business_types",
    "sub_place_of_perform_city_name",
    "sub_place_of_perform_state_code",
    "sub_place_of_perform_state_name",
    "sub_place_of_performance_zip",
    "sub_place_of_performance_county_code",
    "sub_place_of_performance_county_name",
    "sub_place_of_perform_congressio",
    "sub_place_of_perform_country_co",
    "sub_place_of_perform_country_na",
    "subaward_description",
    "sub_high_comp_officer1_full_na",
    "sub_high_comp_officer1_amount",
    "sub_high_comp_officer2_full_na",
    "sub_high_comp_officer2_amount",
    "sub_high_comp_officer3_full_na",
    "sub_high_comp_officer3_amount",
    "sub_high_comp_officer4_full_na",
    "sub_high_comp_officer4_amount",
    "sub_high_comp_officer5_full_na",
    "sub_high_comp_officer5_amount",
    "sub_id",
    "sub_parent_id",
    "sub_federal_agency_id",
    "sub_federal_agency_name",
    "sub_funding_agency_id",
    "sub_funding_agency_name",
    "sub_funding_office_id",
    "sub_funding_office_name",
    "sub_naics",
    "sub_cfda_numbers",
    "sub_dunsplus4",
    "sub_recovery_subcontract_amt",
    "sub_recovery_model_q1",
    "sub_recovery_model_q2",
    "sub_compensation_q1",
    "sub_compensation_q2",
    "sub_place_of_perform_street",
    "created_at",
    "updated_at"
)
SELECT
    -- File F Prime Awards
    lap.unique_award_key AS "unique_award_key",
    fsrs_grant.fain AS "award_id",
    NULL AS "parent_award_id",
    gap.award_amount AS "award_amount",
    bap.action_date AS "action_date",
    'FY' || fy(bap.action_date) AS "fy",
    lap.awarding_agency_code AS "awarding_agency_code",
    lap.awarding_agency_name AS "awarding_agency_name",
    fsrs_grant.federal_agency_id AS "awarding_sub_tier_agency_c",
    lap.awarding_sub_tier_agency_n AS "awarding_sub_tier_agency_n",
    lap.awarding_office_code AS "awarding_office_code",
    lap.awarding_office_name AS "awarding_office_name",
    lap.funding_agency_code AS "funding_agency_code",
    lap.funding_agency_name AS "funding_agency_name",
    lap.funding_sub_tier_agency_co AS "funding_sub_tier_agency_co",
    lap.funding_sub_tier_agency_na AS "funding_sub_tier_agency_na",
    lap.funding_office_code AS "funding_office_code",
    lap.funding_office_name AS "funding_office_name",
    lap.awardee_or_recipient_uniqu AS "awardee_or_recipient_uniqu",
    lap.uei AS "awardee_or_recipient_uei",
    lap.awardee_or_recipient_legal AS "awardee_or_recipient_legal",
    grant_uei.dba_name AS "dba_name",
    lap.ultimate_parent_unique_ide AS "ultimate_parent_unique_ide",
    lap.ultimate_parent_uei AS "ultimate_parent_uei",
    lap.ultimate_parent_legal_enti AS "ultimate_parent_legal_enti",
    le_country.country_code AS "legal_entity_country_code",
    le_country.country_name AS "legal_entity_country_name",
    lap.legal_entity_address_line1 AS "legal_entity_address_line1",
    lap.legal_entity_city_name AS "legal_entity_city_name",
    lap.legal_entity_state_code AS "legal_entity_state_code",
    lap.legal_entity_state_name AS "legal_entity_state_name",
    CASE WHEN lap.legal_entity_country_code = 'USA'
         THEN lap.legal_entity_zip
         ELSE NULL
    END AS "legal_entity_zip",
    lap.legal_entity_county_code AS "legal_entity_county_code",
    lap.legal_entity_county_name AS "legal_entity_county_name",
    lap.legal_entity_congressional AS "legal_entity_congressional",
    CASE WHEN lap.legal_entity_country_code <> 'USA'
        THEN lap.legal_entity_foreign_posta
        ELSE NULL
    END AS "legal_entity_foreign_posta",
    lap.business_types_desc AS "business_types",
    lap.place_of_performance_city AS "place_of_perform_city_name",
    lap.place_of_perfor_state_code AS "place_of_perform_state_code",
    lap.place_of_perform_state_nam AS "place_of_perform_state_name",
    lap.place_of_performance_zip AS "place_of_performance_zip",
    lap.place_of_perform_county_co AS "place_of_performance_county_code",
    lap.place_of_perform_county_na AS "place_of_performance_county_name",
    lap.place_of_performance_congr AS "place_of_perform_congressio",
    ppop_country.country_code AS "place_of_perform_country_co",
    ppop_country.country_name AS "place_of_perform_country_na",
    bap.award_description AS "award_description",
    NULL AS "naics",
    NULL AS "naics_description",
    ARRAY_TO_STRING(gap.cfda_nums, ', ') AS "cfda_numbers",
    ARRAY_TO_STRING(gap.cfda_names, ', ') AS "cfda_titles",
    fsrs_grant.id AS "prime_id",
    fsrs_grant.internal_id AS "internal_id",
    fsrs_grant.date_submitted AS "date_submitted",
    NULL AS "report_type",
    NULL AS "transaction_type",
    NULL AS "program_title",
    NULL AS "contract_agency_code",
    NULL AS "contract_idv_agency_code",
    fsrs_grant.funding_agency_id AS "grant_funding_agency_id",
    fsrs_grant.funding_agency_name AS "grant_funding_agency_name",
    fsrs_grant.federal_agency_name AS "federal_agency_name",
    NULL AS "treasury_symbol",
    fsrs_grant.dunsplus4 AS "dunsplus4",
    NULL AS "recovery_model_q1",
    NULL AS "recovery_model_q2",
    fsrs_grant.compensation_q1 AS "compensation_q1",
    fsrs_grant.compensation_q2 AS "compensation_q2",
    lap.high_comp_officer1_full_na AS "high_comp_officer1_full_na",
    lap.high_comp_officer1_amount AS "high_comp_officer1_amount",
    lap.high_comp_officer2_full_na AS "high_comp_officer2_full_na",
    lap.high_comp_officer2_amount AS "high_comp_officer2_amount",
    lap.high_comp_officer3_full_na AS "high_comp_officer3_full_na",
    lap.high_comp_officer3_amount AS "high_comp_officer3_amount",
    lap.high_comp_officer4_full_na AS "high_comp_officer4_full_na",
    lap.high_comp_officer4_amount AS "high_comp_officer4_amount",
    lap.high_comp_officer5_full_na AS "high_comp_officer5_full_na",
    lap.high_comp_officer5_amount AS "high_comp_officer5_amount",
    fsrs_grant.principle_place_street AS "place_of_perform_street",

    -- File F Subawards
    'sub-grant' AS "subaward_type",
    fsrs_grant.report_period_year AS "subaward_report_year",
    fsrs_grant.report_period_mon AS "subaward_report_month",
    fsrs_subgrant.subaward_num AS "subaward_number",
    fsrs_subgrant.subaward_amount AS "subaward_amount",
    fsrs_subgrant.subaward_date AS "sub_action_date",
    fsrs_subgrant.duns AS "sub_awardee_or_recipient_uniqu",
    fsrs_subgrant.uei_number AS "sub_awardee_or_recipient_uei",
    fsrs_subgrant.awardee_name AS "sub_awardee_or_recipient_legal",
    fsrs_subgrant.dba_name AS "sub_dba_name",
    fsrs_subgrant.parent_duns AS "sub_ultimate_parent_unique_ide",
    fsrs_subgrant.parent_uei AS "sub_ultimate_parent_uei",
    subgrant_puei.legal_business_name AS "sub_ultimate_parent_legal_enti",
    sub_le_country.country_code AS "sub_legal_entity_country_code",
    sub_le_country.country_name AS "sub_legal_entity_country_name",
    fsrs_subgrant.awardee_address_street AS "sub_legal_entity_address_line1",
    fsrs_subgrant.awardee_address_city AS "sub_legal_entity_city_name",
    fsrs_subgrant.awardee_address_state AS "sub_legal_entity_state_code",
    fsrs_subgrant.awardee_address_state_name AS "sub_legal_entity_state_name",
    CASE WHEN fsrs_subgrant.awardee_address_country = 'USA'
         THEN fsrs_subgrant.awardee_address_zip
         ELSE NULL
    END AS "sub_legal_entity_zip",
    sub_le_county_code.county_number AS "sub_legal_entity_county_code",
    sub_le_county_name.county_name AS "sub_legal_entity_county_name",
    fsrs_subgrant.awardee_address_district AS "sub_legal_entity_congressional",
    CASE WHEN fsrs_subgrant.awardee_address_country <> 'USA'
         THEN fsrs_subgrant.awardee_address_zip
         ELSE NULL
    END AS "sub_legal_entity_foreign_posta",
    CASE WHEN cardinality(subgrant_uei.business_types) > 0
         THEN array_to_string(subgrant_uei.business_types, ',')
         ELSE NULL
    END AS "sub_business_types",
    fsrs_subgrant.principle_place_city AS "sub_place_of_perform_city_name",
    fsrs_subgrant.principle_place_state AS "sub_place_of_perform_state_code",
    fsrs_subgrant.principle_place_state_name AS "sub_place_of_perform_state_name",
    fsrs_subgrant.principle_place_zip AS "sub_place_of_performance_zip",
    sub_ppop_county_code.county_number AS "sub_place_of_performance_county_code",
    sub_ppop_county_name.county_name AS "sub_place_of_performance_county_name",
    fsrs_subgrant.principle_place_district AS "sub_place_of_perform_congressio",
    sub_ppop_country.country_code AS "sub_place_of_perform_country_co",
    sub_ppop_country.country_name AS "sub_place_of_perform_country_na",
    fsrs_subgrant.project_description AS "subaward_description",
    fsrs_subgrant.top_paid_fullname_1 AS "sub_high_comp_officer1_full_na",
    fsrs_subgrant.top_paid_amount_1 AS "sub_high_comp_officer1_amount",
    fsrs_subgrant.top_paid_fullname_2 AS "sub_high_comp_officer2_full_na",
    fsrs_subgrant.top_paid_amount_2 AS "sub_high_comp_officer2_amount",
    fsrs_subgrant.top_paid_fullname_3 AS "sub_high_comp_officer3_full_na",
    fsrs_subgrant.top_paid_amount_3 AS "sub_high_comp_officer3_amount",
    fsrs_subgrant.top_paid_fullname_4 AS "sub_high_comp_officer4_full_na",
    fsrs_subgrant.top_paid_amount_4 AS "sub_high_comp_officer4_amount",
    fsrs_subgrant.top_paid_fullname_5 AS "sub_high_comp_officer5_full_na",
    fsrs_subgrant.top_paid_amount_5 AS "sub_high_comp_officer5_amount",
    fsrs_subgrant.id AS "sub_id",
    fsrs_subgrant.parent_id AS "sub_parent_id",
    fsrs_subgrant.federal_agency_id AS "sub_federal_agency_id",
    fsrs_subgrant.federal_agency_name AS "sub_federal_agency_name",
    fsrs_subgrant.funding_agency_id AS "sub_funding_agency_id",
    fsrs_subgrant.funding_agency_name AS "sub_funding_agency_name",
    NULL AS "sub_funding_office_id",
    NULL AS "sub_funding_office_name",
    NULL AS "sub_naics",
    fsrs_subgrant.cfda_numbers AS "sub_cfda_numbers",
    fsrs_subgrant.dunsplus4 AS "sub_dunsplus4",
    NULL AS "sub_recovery_subcontract_amt",
    NULL AS "sub_recovery_model_q1",
    NULL AS "sub_recovery_model_q2",
    fsrs_subgrant.compensation_q1 AS "sub_compensation_q1",
    fsrs_subgrant.compensation_q2 AS "sub_compensation_q2",
    fsrs_subgrant.principle_place_street AS "sub_place_of_perform_street",

    NOW() AS "created_at",
    NOW() AS "updated_at"

FROM fsrs_grant
    JOIN fsrs_subgrant
        ON fsrs_subgrant.parent_id = fsrs_grant.id
    LEFT OUTER JOIN latest_aw_pf AS lap
        ON UPPER(TRANSLATE(fsrs_grant.fain, '-', '')) = UPPER(TRANSLATE(lap.fain, '-', ''))
        AND UPPER(fsrs_grant.federal_agency_id) IS NOT DISTINCT FROM UPPER(lap.awarding_sub_tier_agency_c)
    LEFT OUTER JOIN base_aw_pf AS bap
        ON UPPER(TRANSLATE(fsrs_grant.fain, '-', '')) = UPPER(TRANSLATE(bap.fain, '-', ''))
        AND UPPER(fsrs_grant.federal_agency_id) IS NOT DISTINCT FROM UPPER(bap.awarding_sub_tier_agency_c)
    LEFT OUTER JOIN grouped_aw_pf AS gap
        ON UPPER(TRANSLATE(fsrs_grant.fain, '-', '')) = UPPER(TRANSLATE(gap.fain, '-', ''))
        AND UPPER(fsrs_grant.federal_agency_id) IS NOT DISTINCT FROM UPPER(gap.awarding_sub_tier_agency_c)
    LEFT OUTER JOIN country_code AS le_country
        ON (UPPER(fsrs_grant.awardee_address_country) = UPPER(le_country.country_code)
            OR UPPER(fsrs_grant.awardee_address_country) = UPPER(le_country.country_code_2_char))
    LEFT OUTER JOIN country_code AS ppop_country
        ON (UPPER(fsrs_grant.principle_place_country) = UPPER(ppop_country.country_code)
            OR UPPER(fsrs_grant.principle_place_country) = UPPER(ppop_country.country_code_2_char))
    LEFT OUTER JOIN country_code AS sub_le_country
        ON (UPPER(fsrs_subgrant.awardee_address_country) = UPPER(sub_le_country.country_code)
            OR UPPER(fsrs_subgrant.awardee_address_country) = UPPER(sub_le_country.country_code_2_char))
    LEFT OUTER JOIN zips_modified_union AS sub_le_county_code
        ON (fsrs_subgrant.awardee_address_country = 'USA' AND (
            (
                fsrs_subgrant.awardee_address_zip = sub_le_county_code.sub_zip
                AND sub_le_county_code.type = 'zip9'
            )
            OR
            (
                LEFT(fsrs_subgrant.awardee_address_zip, 5) = sub_le_county_code.sub_zip
                AND fsrs_subgrant.awardee_address_state = sub_le_county_code.state_abbreviation
                AND sub_le_county_code.type = 'zip5+state'
            )
        ))
    LEFT OUTER JOIN county_code AS sub_le_county_name
    	ON (sub_le_county_code.county_number = sub_le_county_name.county_number
    		AND sub_le_county_code.state_abbreviation = sub_le_county_name.state_code)
    LEFT OUTER JOIN country_code AS sub_ppop_country
        ON (UPPER(fsrs_subgrant.principle_place_country) = UPPER(sub_ppop_country.country_code)
            OR UPPER(fsrs_subgrant.principle_place_country) = UPPER(sub_ppop_country.country_code_2_char))
    LEFT OUTER JOIN zips_modified_union AS sub_ppop_county_code
        ON (sub_ppop_country.country_code = 'USA' AND (
            (
            	fsrs_subgrant.principle_place_zip = sub_ppop_county_code.sub_zip
                AND sub_ppop_county_code.type = 'zip9'
	        )
	        OR
	        (
            	LEFT(fsrs_subgrant.principle_place_zip, 5) = sub_ppop_county_code.sub_zip
            	AND fsrs_subgrant.principle_place_state = sub_ppop_county_code.state_abbreviation
                AND sub_ppop_county_code.type = 'zip5+state'
           	)
        ))
    LEFT OUTER JOIN county_code AS sub_ppop_county_name
    	ON (sub_ppop_county_code.county_number = sub_ppop_county_name.county_number
    		AND sub_ppop_county_code.state_abbreviation = sub_ppop_county_name.state_code)
    LEFT OUTER JOIN grant_uei
        ON UPPER(fsrs_grant.uei_number) = UPPER(grant_uei.uei)
    LEFT OUTER JOIN subgrant_puei
        ON UPPER(fsrs_subgrant.parent_uei) = UPPER(subgrant_puei.uei)
    LEFT OUTER JOIN subgrant_uei
        ON UPPER(fsrs_subgrant.uei_number) = UPPER(subgrant_uei.uei)
WHERE fsrs_grant.id {0} {1};
