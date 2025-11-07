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
        UPPER(pf.unique_award_key) AS unique_award_key,
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
        UPPER(pf.assistance_listing_number) AS assistance_listing_number,
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
            FROM sam_subgrant
            WHERE {0}
                AND UPPER(pf.unique_award_key) = UPPER(sam_subgrant.unique_award_key)
        )
    );
CREATE INDEX ix_aw_pf_uak ON aw_pf (unique_award_key);
CREATE INDEX ix_aw_pf_act_date ON aw_pf (action_date);
CREATE INDEX ix_aw_pf_act_date_desc ON aw_pf (action_date DESC);
CREATE INDEX ix_aw_pf_act_type ON aw_pf (action_type_sort);
CREATE INDEX ix_aw_pf_act_type_desc ON aw_pf (action_type_sort DESC);
CREATE INDEX ix_aw_pf_mod_num_sort ON aw_pf (mod_num_sort);
CREATE INDEX ix_aw_pf_mod_num_sort_desc ON aw_pf (mod_num_sort DESC);
ANALYZE aw_pf;

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
ANALYZE base_aw_pf;

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
ANALYZE latest_aw_pf;

CREATE TEMPORARY TABLE grouped_aw_pf ON COMMIT DROP AS
    (SELECT pf.unique_award_key,
        array_agg(DISTINCT pf.assistance_listing_number) AS assistance_listing_nums,
        array_agg(DISTINCT al.program_title) AS assistance_listing_names,
        SUM(pf.federal_action_obligation) AS award_amount
     FROM aw_pf AS pf
     LEFT OUTER JOIN assistance_listing AS al
        ON UPPER(al.program_number) = pf.assistance_listing_number
     GROUP BY unique_award_key
     );
CREATE INDEX ix_grouped_aw_pf_uak ON grouped_aw_pf (unique_award_key);
ANALYZE grouped_aw_pf;

CREATE TEMPORARY TABLE grant_uei ON COMMIT DROP AS
    (SELECT grant_uei_from.uei AS uei,
        grant_uei_from.duns AS duns,
        grant_uei_from.legal_business_name AS legal_business_name,
        grant_uei_from.dba_name AS dba_name
    FROM (
        SELECT sam_recipient.uei AS uei,
            sam_recipient.awardee_or_recipient_uniqu AS duns,
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
ANALYZE grant_uei;

CREATE TEMPORARY TABLE subgrant_puei ON COMMIT DROP AS (
    SELECT sub_puei_from.uei AS uei,
        sub_puei_from.duns AS duns,
        sub_puei_from.legal_business_name AS legal_business_name
    FROM (
        SELECT sam_recipient.uei AS uei,
            sam_recipient.awardee_or_recipient_uniqu AS duns,
            sam_recipient.legal_business_name AS legal_business_name,
            row_number() OVER (PARTITION BY
                UPPER(sam_recipient.uei)
            ) AS row
        FROM sam_subgrant
            LEFT OUTER JOIN sam_recipient
                ON UPPER(sam_subgrant.parent_uei) = UPPER(sam_recipient.uei)
                AND {0}
        ORDER BY sam_recipient.activation_date DESC
    ) AS sub_puei_from
    WHERE sub_puei_from.row = 1
    );
CREATE INDEX ix_subgrant_puei_upp ON subgrant_puei (UPPER(uei));
ANALYZE subgrant_puei;

CREATE TEMPORARY TABLE subgrant_uei ON COMMIT DROP AS (
    SELECT sub_uei_from.uei AS uei,
        sub_uei_from.duns AS duns,
        sub_uei_from.business_types AS business_types
    FROM (
        SELECT
            sam_recipient.uei AS uei,
            sam_recipient.awardee_or_recipient_uniqu AS duns,
            sam_recipient.business_types AS business_types,
            row_number() OVER (PARTITION BY
                UPPER(sam_recipient.uei)
            ) AS row
        FROM sam_subgrant
            LEFT OUTER JOIN sam_recipient
                ON UPPER(sam_subgrant.uei) = UPPER(sam_recipient.uei)
                AND {0}
        ORDER BY sam_recipient.activation_date DESC
    ) AS sub_uei_from
    WHERE sub_uei_from.row = 1
    );
CREATE INDEX ix_subgrant_uei_upp ON subgrant_uei (UPPER(uei));
ANALYZE subgrant_uei;

-- Getting a list of all the subaward zips we'll encounter to limit any massive joins
CREATE TEMPORARY TABLE all_sub_zips ON COMMIT DROP AS (
	SELECT DISTINCT legal_entity_zip_code AS "sub_zip"
	FROM sam_subgrant
	UNION
	SELECT DISTINCT ppop_zip_code AS "sub_zip"
	FROM sam_subgrant
);
ANALYZE all_sub_zips;
-- Matching on all the available zip9s
CREATE TEMPORARY TABLE all_sub_zip9s ON COMMIT DROP AS (
	SELECT sub_zip
	FROM all_sub_zips
	WHERE LENGTH(sub_zip) = 9
);
ANALYZE all_sub_zip9s;
CREATE TEMPORARY TABLE modified_zips ON COMMIT DROP AS (
	SELECT (zip5 || zip_last4) AS "sub_zip", county_number, state_abbreviation
	FROM zips
	WHERE EXISTS (
		SELECT 1
		FROM all_sub_zip9s AS asz
		WHERE (zip5 || zip_last4) = asz.sub_zip
	)
);
ANALYZE modified_zips;
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
ANALYZE all_sub_zip5s;

-- Since counties can vary between a zip5 + state, we want to only match on when there's only one county and not guess
CREATE TEMPORARY TABLE single_zips_grouped ON COMMIT DROP AS (
	SELECT zip5, state_abbreviation
	FROM zips_grouped
	GROUP BY zip5, state_abbreviation
	HAVING COUNT(*) = 1
);
ANALYZE single_zips_grouped;
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
ANALYZE zips_grouped_modified;
-- Combine the two matching groups together and join later. make sure keep them separated with type to prevent dups
CREATE TEMPORARY TABLE zips_modified_union ON COMMIT DROP AS (
	SELECT sub_zip, state_abbreviation, county_number, 'zip9' AS "zip_type"
	FROM modified_zips
	UNION
	SELECT zip5 AS "sub_zip", state_abbreviation, county_number, 'zip5+state' AS "zip_type"
	FROM zips_grouped_modified
);
CREATE INDEX ix_zmu_sz ON zips_modified_union (sub_zip);
CREATE INDEX ix_zmu_type ON zips_modified_union (zip_type);
ANALYZE zips_modified_union;

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
    "assistance_listing_numbers",
    "assistance_listing_titles",
    "prime_id",
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
    "internal_id",
    "date_submitted",
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
    "sub_assistance_listing_numbers",
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
    lap.fain AS "award_id",
    NULL AS "parent_award_id",
    gap.award_amount AS "award_amount",
    bap.action_date AS "action_date",
    'FY' || fy(bap.action_date) AS "fy",
    lap.awarding_agency_code AS "awarding_agency_code",
    lap.awarding_agency_name AS "awarding_agency_name",
    lap.awarding_sub_tier_agency_c AS "awarding_sub_tier_agency_c",
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
    lap.legal_entity_country_code AS "legal_entity_country_code",
    lap.legal_entity_country_name AS "legal_entity_country_name",
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
    -- note: these have a semicolon separator, not a comma separator
    lap.business_types_desc AS "business_types",
    lap.place_of_performance_city AS "place_of_perform_city_name",
    lap.place_of_perfor_state_code AS "place_of_perform_state_code",
    lap.place_of_perform_state_nam AS "place_of_perform_state_name",
    lap.place_of_performance_zip AS "place_of_performance_zip",
    lap.place_of_perform_county_co AS "place_of_performance_county_code",
    lap.place_of_perform_county_na AS "place_of_performance_county_name",
    lap.place_of_performance_congr AS "place_of_perform_congressio",
    lap.place_of_perform_country_c AS "place_of_perform_country_co",
    lap.place_of_perform_country_n AS "place_of_perform_country_na",
    bap.award_description AS "award_description",
    NULL AS "naics",
    NULL AS "naics_description",
    ARRAY_TO_STRING(gap.assistance_listing_nums, ', ') AS "assistance_listing_numbers",
    ARRAY_TO_STRING(gap.assistance_listing_names, ', ') AS "assistance_listing_titles",

    NULL AS "prime_id",
    NULL AS "report_type",
    NULL AS "transaction_type",
    NULL AS "program_title",
    NULL AS "contract_agency_code",
    NULL AS "contract_idv_agency_code",
    lap.funding_sub_tier_agency_co AS "grant_funding_agency_id",
    lap.funding_sub_tier_agency_na AS "grant_funding_agency_name",
    lap.awarding_sub_tier_agency_c AS "federal_agency_name",
    NULL AS "treasury_symbol",
    NULL AS "dunsplus4",
    NULL AS "recovery_model_q1",
    NULL AS "recovery_model_q2",
    NULL AS "compensation_q1",
    NULL AS "compensation_q2",
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
    NULL AS "place_of_perform_street",

    -- File F Subawards
    'sub-grant' AS "subaward_type",
    sam_subgrant.subaward_report_number AS "internal_id",
    sam_subgrant.date_submitted AS "date_submitted",
    EXTRACT(YEAR FROM CAST(sam_subgrant.date_submitted AS DATE)) AS "subaward_report_year",
    LPAD(CAST(EXTRACT(MONTH FROM CAST(sam_subgrant.date_submitted AS DATE)) AS CHAR(2)), 2, '0') AS "subaward_report_month",
    sam_subgrant.award_number AS "subaward_number",
    sam_subgrant.award_amount AS "subaward_amount",
    sam_subgrant.action_date AS "sub_action_date",
    subgrant_uei.duns AS "sub_awardee_or_recipient_uniqu",
    sam_subgrant.uei AS "sub_awardee_or_recipient_uei",
    sam_subgrant.legal_business_name AS "sub_awardee_or_recipient_legal",
    sam_subgrant.dba_name AS "sub_dba_name",
    subgrant_puei.duns AS "sub_ultimate_parent_unique_ide",
    sam_subgrant.parent_uei AS "sub_ultimate_parent_uei",
    sam_subgrant.parent_legal_business_name AS "sub_ultimate_parent_legal_enti",
    sam_subgrant.legal_entity_country_code AS "sub_legal_entity_country_code",
    sam_subgrant.legal_entity_country_name AS "sub_legal_entity_country_name",
    sam_subgrant.legal_entity_address_line1 AS "sub_legal_entity_address_line1",
    sam_subgrant.legal_entity_city_name AS "sub_legal_entity_city_name",
    CASE WHEN UPPER(sam_subgrant.legal_entity_state_code) <> 'ZZ'
        THEN  sam_subgrant.legal_entity_state_code
        ELSE NULL
    END AS "sub_legal_entity_state_code",
    CASE WHEN UPPER(sam_subgrant.legal_entity_state_code) <> 'ZZ'
        THEN  sam_subgrant.legal_entity_state_name
        ELSE NULL
    END AS "sub_legal_entity_state_name",
    CASE WHEN UPPER(sam_subgrant.legal_entity_country_code) = 'USA'
         THEN sam_subgrant.legal_entity_zip_code
         ELSE NULL
    END AS "sub_legal_entity_zip",
    COALESCE(sub_le_county_code_zip9.county_number, sub_le_county_code_zip5.county_number) AS "sub_legal_entity_county_code",
    sub_le_county_name.county_name AS "sub_legal_entity_county_name",
    sam_subgrant.legal_entity_congressional AS "sub_legal_entity_congressional",
    CASE WHEN UPPER(sam_subgrant.legal_entity_country_code) <> 'USA'
         THEN sam_subgrant.legal_entity_zip_code
         ELSE NULL
    END AS "sub_legal_entity_foreign_posta",
    CASE WHEN cardinality(sam_subgrant.business_types_names) > 0
     THEN array_to_string(sam_subgrant.business_types_names, ',')
     ELSE NULL
    END AS "sub_business_types",
    sam_subgrant.ppop_city_name AS "sub_place_of_perform_city_name",
    CASE WHEN UPPER(sam_subgrant.ppop_state_code) <> 'ZZ'
        THEN  sam_subgrant.ppop_state_code
        ELSE NULL
    END AS "sub_place_of_perform_state_code",
    CASE WHEN UPPER(sam_subgrant.ppop_state_code) <> 'ZZ'
        THEN  sam_subgrant.ppop_state_name
        ELSE NULL
    END AS "sub_place_of_perform_state_name",
    sam_subgrant.ppop_zip_code AS "sub_place_of_performance_zip",
    COALESCE(sub_ppop_county_code_zip9.county_number, sub_ppop_county_code_zip5.county_number) AS "sub_place_of_performance_county_code",
    sub_ppop_county_name.county_name AS "sub_place_of_performance_county_name",
    sam_subgrant.ppop_congressional_district AS "sub_place_of_perform_congressio",
    sam_subgrant.ppop_country_code AS "sub_place_of_perform_country_co",
    sam_subgrant.ppop_country_name AS "sub_place_of_perform_country_na",
    sam_subgrant.description AS "subaward_description",
    sam_subgrant.high_comp_officer1_full_na AS "sub_high_comp_officer1_full_na",
    sam_subgrant.high_comp_officer1_amount AS "sub_high_comp_officer1_amount",
    sam_subgrant.high_comp_officer2_full_na AS "sub_high_comp_officer2_full_na",
    sam_subgrant.high_comp_officer2_amount AS "sub_high_comp_officer2_amount",
    sam_subgrant.high_comp_officer3_full_na AS "sub_high_comp_officer3_full_na",
    sam_subgrant.high_comp_officer3_amount AS "sub_high_comp_officer3_amount",
    sam_subgrant.high_comp_officer4_full_na AS "sub_high_comp_officer4_full_na",
    sam_subgrant.high_comp_officer4_amount AS "sub_high_comp_officer4_amount",
    sam_subgrant.high_comp_officer5_full_na AS "sub_high_comp_officer5_full_na",
    sam_subgrant.high_comp_officer5_amount AS "sub_high_comp_officer5_amount",
    sam_subgrant.subaward_report_id AS "sub_id",
    NULL AS "sub_parent_id",
    lap.awarding_sub_tier_agency_c AS "sub_federal_agency_id",
    lap.awarding_sub_tier_agency_n AS "sub_federal_agency_name",
    lap.funding_sub_tier_agency_co AS "sub_funding_agency_id",
    lap.funding_sub_tier_agency_na AS "sub_funding_agency_name",
    NULL AS "sub_funding_office_id",
    NULL AS "sub_funding_office_name",
    NULL AS "sub_naics",
    ARRAY_TO_STRING(gap.assistance_listing_nums, ', ') AS "sub_assistance_listing_numbers",
    NULL AS "sub_dunsplus4",
    NULL AS "sub_recovery_subcontract_amt",
    NULL AS "sub_recovery_model_q1",
    NULL AS "sub_recovery_model_q2",
    NULL AS "sub_compensation_q1",
    NULL AS "sub_compensation_q2",
    sam_subgrant.ppop_address_line1 AS "sub_place_of_perform_street",

    NOW() AS "created_at",
    NOW() AS "updated_at"

FROM sam_subgrant
    LEFT OUTER JOIN latest_aw_pf AS lap
        ON UPPER(sam_subgrant.unique_award_key) = lap.unique_award_key
    LEFT OUTER JOIN base_aw_pf AS bap
        ON UPPER(sam_subgrant.unique_award_key) = bap.unique_award_key
    LEFT OUTER JOIN grouped_aw_pf AS gap
        ON UPPER(sam_subgrant.unique_award_key) = gap.unique_award_key
    LEFT OUTER JOIN grant_uei
        ON UPPER(lap.uei) = UPPER(grant_uei.uei)
    LEFT OUTER JOIN subgrant_uei
        ON UPPER(sam_subgrant.uei) = UPPER(subgrant_uei.uei)
    LEFT OUTER JOIN subgrant_puei
        ON UPPER(sam_subgrant.parent_uei) = UPPER(subgrant_puei.uei)
    LEFT OUTER JOIN zips_modified_union AS sub_le_county_code_zip9
        ON (UPPER(sam_subgrant.legal_entity_country_code) = 'USA'
            AND sam_subgrant.legal_entity_zip_code = sub_le_county_code_zip9.sub_zip
            AND sub_le_county_code_zip9.zip_type = 'zip9')
    LEFT OUTER JOIN zips_modified_union AS sub_le_county_code_zip5
        ON (UPPER(sam_subgrant.legal_entity_country_code) = 'USA'
            AND LEFT(sam_subgrant.legal_entity_zip_code, 5) = sub_le_county_code_zip5.sub_zip
            AND UPPER(sam_subgrant.legal_entity_state_code) = sub_le_county_code_zip5.state_abbreviation
            AND sub_le_county_code_zip5.zip_type = 'zip5+state')
    LEFT OUTER JOIN county_code AS sub_le_county_name
    	ON (COALESCE(sub_le_county_code_zip9.county_number, sub_le_county_code_zip5.county_number) = sub_le_county_name.county_number
    		AND COALESCE(sub_le_county_code_zip9.state_abbreviation, sub_le_county_code_zip5.state_abbreviation) = sub_le_county_name.state_code)
    LEFT OUTER JOIN zips_modified_union AS sub_ppop_county_code_zip9
        ON (UPPER(LEFT(sam_subgrant.ppop_country_code, 2)) = 'US'
            AND sam_subgrant.ppop_zip_code = sub_ppop_county_code_zip9.sub_zip
            AND sub_ppop_county_code_zip9.zip_type = 'zip9')
    LEFT OUTER JOIN zips_modified_union AS sub_ppop_county_code_zip5
        ON (UPPER(LEFT(sam_subgrant.ppop_country_code, 2)) = 'US'
            AND LEFT(sam_subgrant.ppop_zip_code, 5) = sub_ppop_county_code_zip5.sub_zip
            AND UPPER(sam_subgrant.ppop_state_code) = sub_ppop_county_code_zip5.state_abbreviation
            AND sub_ppop_county_code_zip5.zip_type = 'zip5+state')
    LEFT OUTER JOIN county_code AS sub_ppop_county_name
    	ON (COALESCE(sub_ppop_county_code_zip9.county_number, sub_ppop_county_code_zip5.county_number) = sub_ppop_county_name.county_number
    		AND COALESCE(sub_ppop_county_code_zip9.state_abbreviation, sub_ppop_county_code_zip5.state_abbreviation) = sub_ppop_county_name.state_code)
WHERE {0};
