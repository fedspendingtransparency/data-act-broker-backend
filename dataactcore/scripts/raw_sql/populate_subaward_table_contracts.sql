CREATE TEMPORARY TABLE aw_dap ON COMMIT DROP AS
    (SELECT dap.unique_award_key AS unique_award_key,
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
        FROM fsrs_procurement
        WHERE UPPER(TRANSLATE(fsrs_procurement.contract_number, '-', '')) = UPPER(TRANSLATE(dap.piid, '-', ''))
            AND UPPER(TRANSLATE(fsrs_procurement.idv_reference_number, '-', '')) IS NOT DISTINCT FROM UPPER(TRANSLATE(dap.parent_award_id, '-', ''))
            AND UPPER(fsrs_procurement.contracting_office_aid) = UPPER(dap.awarding_sub_tier_agency_c)
            AND fsrs_procurement.id {0} {1}
    ));
CREATE INDEX ix_aw_dap_piid_upp ON aw_dap (UPPER(piid));
CREATE INDEX ix_aw_dap_paid_upp ON aw_dap (UPPER(parent_award_id));
CREATE INDEX ix_aw_dap_subtier_upp ON aw_dap (UPPER(awarding_sub_tier_agency_c));
CREATE INDEX ix_aw_dap_act_date ON aw_dap (action_date);
CREATE INDEX ix_aw_dap_act_date_desc ON aw_dap (action_date DESC);
CREATE INDEX ix_aw_dap_act_type ON aw_dap (action_type_sort);
CREATE INDEX ix_aw_dap_act_type_desc ON aw_dap (action_type_sort DESC);
CREATE INDEX ix_aw_dap_mod_num_sort ON aw_dap (mod_num_sort);
CREATE INDEX ix_aw_dap_mod_num_sort_desc ON aw_dap (mod_num_sort DESC);

CREATE TEMPORARY TABLE base_aw_dap ON COMMIT DROP AS
    (SELECT DISTINCT ON (
            UPPER(dap.piid),
            UPPER(dap.parent_award_id),
            UPPER(dap.awarding_sub_tier_agency_c)
        )
        dap.unique_award_key AS unique_award_key,
        dap.piid AS piid,
        dap.parent_award_id AS parent_award_id,
        dap.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
        dap.idv_type AS idv_type,
        dap.award_description as award_description,
        cast_as_date(dap.action_date) AS action_date
    FROM aw_dap AS dap
    ORDER BY UPPER(dap.piid), UPPER(dap.parent_award_id), UPPER(dap.awarding_sub_tier_agency_c), dap.action_date, dap.action_type_sort, dap.mod_num_sort
    );
CREATE INDEX ix_base_aw_dap_piid_upp_trans ON base_aw_dap (UPPER(TRANSLATE(piid, '-', '')));
CREATE INDEX ix_base_aw_dap_paid_upp_trans ON base_aw_dap (UPPER(TRANSLATE(parent_award_id, '-', '')));
CREATE INDEX ix_base_aw_dap_sub_upp ON base_aw_dap (UPPER(awarding_sub_tier_agency_c));

CREATE TEMPORARY TABLE latest_aw_dap ON COMMIT DROP AS
    (SELECT DISTINCT ON (
            UPPER(dap.piid),
            UPPER(dap.parent_award_id),
            UPPER(dap.awarding_sub_tier_agency_c)
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
    ORDER BY UPPER(dap.piid), UPPER(dap.parent_award_id), UPPER(dap.awarding_sub_tier_agency_c), dap.action_date DESC, dap.action_type_sort DESC, dap.mod_num_sort DESC
    );
CREATE INDEX ix_latest_aw_dap_piid_upp_trans ON latest_aw_dap (UPPER(TRANSLATE(piid, '-', '')));
CREATE INDEX ix_latest_aw_dap_paid_upp_trans ON latest_aw_dap (UPPER(TRANSLATE(parent_award_id, '-', '')));
CREATE INDEX ix_latest_aw_dap_sub_upp ON latest_aw_dap (UPPER(awarding_sub_tier_agency_c));

-- Getting a list of all the subaward zips we'll encounter to limit any massive joins
CREATE TEMPORARY TABLE all_sub_zips ON COMMIT DROP AS (
	SELECT DISTINCT company_address_zip AS "sub_zip"
	FROM fsrs_subcontract
	UNION
	SELECT DISTINCT principle_place_zip AS "sub_zip"
	FROM fsrs_subcontract
);
-- Matching on all the available zip9s
CREATE TEMPORARY TABLE all_sub_zip9s ON COMMIT DROP AS (
	SELECT sub_zip
	FROM all_sub_zips
	WHERE LENGTH(sub_zip) = 9
);
CREATE TEMPORARY TABLE modified_zips ON COMMIT DROP AS (
	SELECT (zip5 || zip_last4) AS "sub_zip", county_number, state_abbreviation
	FROM zips
	WHERE EXISTS (
		SELECT 1
		FROM all_sub_zip9s AS asz
		WHERE (zip5 || zip_last4) = asz.sub_zip
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
	SELECT sub_zip, state_abbreviation, county_number, 'zip9' AS "type"
	FROM modified_zips
	UNION
	SELECT zip5 AS "sub_zip", state_abbreviation, county_number, 'zip5+state' AS "type"
	FROM zips_grouped_modified
);
CREATE INDEX ix_zmu_sz ON zips_modified_union (sub_zip);
CREATE INDEX ix_zmu_type ON zips_modified_union (type);

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
    ldap.unique_award_key AS "unique_award_key",
    fsrs_procurement.contract_number AS "award_id",
    fsrs_procurement.idv_reference_number AS "parent_award_id",
    ldap.total_obligated_amount AS "award_amount",
    bdap.action_date AS "action_date",
    'FY' || fy(bdap.action_date) AS "fy",
    ldap.awarding_agency_code AS "awarding_agency_code",
    ldap.awarding_agency_name AS "awarding_agency_name",
    fsrs_procurement.contracting_office_aid AS "awarding_sub_tier_agency_c",
    ldap.awarding_sub_tier_agency_n AS "awarding_sub_tier_agency_n",
    ldap.awarding_office_code AS "awarding_office_code",
    ldap.awarding_office_name AS "awarding_office_name",
    ldap.funding_agency_code AS "funding_agency_code",
    ldap.funding_agency_name AS "funding_agency_name",
    ldap.funding_sub_tier_agency_co AS "funding_sub_tier_agency_co",
    ldap.funding_sub_tier_agency_na AS "funding_sub_tier_agency_na",
    ldap.funding_office_code AS "funding_office_code",
    ldap.funding_office_name AS "funding_office_name",
    ldap.awardee_or_recipient_uniqu AS "awardee_or_recipient_uniqu",
    ldap.awardee_or_recipient_uei AS "awardee_or_recipient_uei",
    ldap.awardee_or_recipient_legal AS "awardee_or_recipient_legal",
    ldap.vendor_doing_as_business_n AS "dba_name",
    ldap.ultimate_parent_unique_ide AS "ultimate_parent_unique_ide",
    ldap.ultimate_parent_uei AS "ultimate_parent_uei",
    ldap.ultimate_parent_legal_enti AS "ultimate_parent_legal_enti",
    ldap.legal_entity_country_code AS "legal_entity_country_code",
    ldap.legal_entity_country_name AS "legal_entity_country_name",
    ldap.legal_entity_address_line1 AS "legal_entity_address_line1",
    ldap.legal_entity_city_name AS "legal_entity_city_name",
    ldap.legal_entity_state_code AS "legal_entity_state_code",
    ldap.legal_entity_state_descrip AS "legal_entity_state_name",
    CASE WHEN ldap.legal_entity_country_code = 'USA'
         THEN ldap.legal_entity_zip4
         ELSE NULL
    END AS "legal_entity_zip",
    ldap.legal_entity_county_code AS "legal_entity_county_code",
    ldap.legal_entity_county_name AS "legal_entity_county_name",
    ldap.legal_entity_congressional AS "legal_entity_congressional",
    CASE WHEN ldap.legal_entity_country_code <> 'USA'
         THEN ldap.legal_entity_zip4
         ELSE NULL
    END AS "legal_entity_foreign_posta",
    fsrs_procurement.bus_types AS "business_types",
    ldap.place_of_perform_city_name AS "place_of_perform_city_name",
    ldap.place_of_performance_state AS "place_of_perform_state_code",
    ldap.place_of_perfor_state_desc AS "place_of_perform_state_name",
    ldap.place_of_performance_zip4a AS "place_of_performance_zip",
    ldap.place_of_perform_county_co AS "place_of_performance_county_code",
    ldap.place_of_perform_county_na AS "place_of_performance_county_name",
    ldap.place_of_performance_congr AS "place_of_perform_congressio",
    ldap.place_of_perform_country_c AS "place_of_perform_country_co",
    ldap.place_of_perf_country_desc AS "place_of_perform_country_na",
    bdap.award_description AS "award_description",
    ldap.naics AS "naics",
    ldap.naics_description AS "naics_description",
    NULL AS "cfda_numbers",
    NULL AS "cfda_titles",
    fsrs_procurement.id AS "prime_id",
    fsrs_procurement.internal_id AS "internal_id",
    fsrs_procurement.date_submitted AS "date_submitted",
    fsrs_procurement.report_type AS "report_type",
    fsrs_procurement.transaction_type AS "transaction_type",
    fsrs_procurement.program_title AS "program_title",
    fsrs_procurement.contract_agency_code AS "contract_agency_code",
    fsrs_procurement.contract_idv_agency_code AS "contract_idv_agency_code",
    NULL AS "grant_funding_agency_id",
    NULL AS "grant_funding_agency_name",
    NULL AS "federal_agency_name",
    fsrs_procurement.treasury_symbol AS "treasury_symbol",
    NULL AS "dunsplus4",
    fsrs_procurement.recovery_model_q1 AS "recovery_model_q1",
    fsrs_procurement.recovery_model_q2 AS "recovery_model_q2",
    NULL AS "compensation_q1",
    NULL AS "compensation_q2",
    ldap.high_comp_officer1_full_na AS "high_comp_officer1_full_na",
    ldap.high_comp_officer1_amount AS "high_comp_officer1_amount",
    ldap.high_comp_officer2_full_na AS "high_comp_officer2_full_na",
    ldap.high_comp_officer2_amount AS "high_comp_officer2_amount",
    ldap.high_comp_officer3_full_na AS "high_comp_officer3_full_na",
    ldap.high_comp_officer3_amount AS "high_comp_officer3_amount",
    ldap.high_comp_officer4_full_na AS "high_comp_officer4_full_na",
    ldap.high_comp_officer4_amount AS "high_comp_officer4_amount",
    ldap.high_comp_officer5_full_na AS "high_comp_officer5_full_na",
    ldap.high_comp_officer5_amount AS "high_comp_officer5_amount",
    fsrs_procurement.principle_place_street AS "place_of_perform_street",

    -- File F Subawards
    'sub-contract' AS "subaward_type",
    fsrs_procurement.report_period_year AS "subaward_report_year",
    fsrs_procurement.report_period_mon AS "subaward_report_month",
    fsrs_subcontract.subcontract_num AS "subaward_number",
    fsrs_subcontract.subcontract_amount AS "subaward_amount",
    fsrs_subcontract.subcontract_date AS "sub_action_date",
    fsrs_subcontract.duns AS "sub_awardee_or_recipient_uniqu",
    fsrs_subcontract.uei_number AS "sub_awardee_or_recipient_uei",
    fsrs_subcontract.company_name AS "sub_awardee_or_recipient_legal",
    fsrs_subcontract.dba_name AS "sub_dba_name",
    fsrs_subcontract.parent_duns AS "sub_ultimate_parent_unique_ide",
    fsrs_subcontract.parent_uei AS "sub_ultimate_parent_uei",
    fsrs_subcontract.parent_company_name AS "sub_ultimate_parent_legal_enti",
    sub_le_country.country_code AS "sub_legal_entity_country_code",
    sub_le_country.country_name AS "sub_legal_entity_country_name",
    fsrs_subcontract.company_address_street AS "sub_legal_entity_address_line1",
    fsrs_subcontract.company_address_city AS "sub_legal_entity_city_name",
    fsrs_subcontract.company_address_state AS "sub_legal_entity_state_code",
    fsrs_subcontract.company_address_state_name AS "sub_legal_entity_state_name",
    CASE WHEN fsrs_subcontract.company_address_country = 'USA'
         THEN fsrs_subcontract.company_address_zip
         ELSE NULL
    END AS "sub_legal_entity_zip",
    COALESCE(sub_le_county_code_zip9.county_number, sub_le_county_code_zip5.county_number) AS "sub_legal_entity_county_code",
    sub_le_county_name.county_name AS "sub_legal_entity_county_name",
    fsrs_subcontract.company_address_district AS "sub_legal_entity_congressional",
    CASE WHEN fsrs_subcontract.company_address_country <> 'USA'
         THEN fsrs_subcontract.company_address_zip
         ELSE NULL
    END AS "sub_legal_entity_foreign_posta",
    fsrs_subcontract.bus_types AS "sub_business_types",
    fsrs_subcontract.principle_place_city AS "sub_place_of_perform_city_name",
    fsrs_subcontract.principle_place_state AS "sub_place_of_perform_state_code",
    fsrs_subcontract.principle_place_state_name AS "sub_place_of_perform_state_name",
    fsrs_subcontract.principle_place_zip AS "sub_place_of_performance_zip",
    COALESCE(sub_ppop_county_code_zip9.county_number, sub_ppop_county_code_zip5.county_number) AS "sub_place_of_performance_county_code",
    sub_ppop_county_name.county_name AS "sub_place_of_performance_county_name",
    fsrs_subcontract.principle_place_district AS "sub_place_of_perform_congressio",
    sub_ppop_country.country_code AS "sub_place_of_perform_country_co",
    sub_ppop_country.country_name AS "sub_place_of_perform_country_na",
    fsrs_subcontract.overall_description AS "subaward_description",
    fsrs_subcontract.top_paid_fullname_1 AS "sub_high_comp_officer1_full_na",
    fsrs_subcontract.top_paid_amount_1 AS "sub_high_comp_officer1_amount",
    fsrs_subcontract.top_paid_fullname_2 AS "sub_high_comp_officer2_full_na",
    fsrs_subcontract.top_paid_amount_2 AS "sub_high_comp_officer2_amount",
    fsrs_subcontract.top_paid_fullname_3 AS "sub_high_comp_officer3_full_na",
    fsrs_subcontract.top_paid_amount_3 AS "sub_high_comp_officer3_amount",
    fsrs_subcontract.top_paid_fullname_4 AS "sub_high_comp_officer4_full_na",
    fsrs_subcontract.top_paid_amount_4 AS "sub_high_comp_officer4_amount",
    fsrs_subcontract.top_paid_fullname_5 AS "sub_high_comp_officer5_full_na",
    fsrs_subcontract.top_paid_amount_5 AS "sub_high_comp_officer5_amount",
    fsrs_subcontract.id AS "sub_id",
    fsrs_subcontract.parent_id AS "sub_parent_id",
    NULL AS "sub_federal_agency_id",
    NULL AS "sub_federal_agency_name",
    fsrs_subcontract.funding_agency_id AS "sub_funding_agency_id",
    fsrs_subcontract.funding_agency_name AS "sub_funding_agency_name",
    fsrs_subcontract.funding_office_id AS "sub_funding_office_id",
    fsrs_subcontract.funding_office_name AS "sub_funding_office_name",
    fsrs_subcontract.naics AS "sub_naics",
    NULL AS "sub_cfda_numbers",
    NULL AS "sub_dunsplus4",
    fsrs_subcontract.recovery_subcontract_amt AS "sub_recovery_subcontract_amt",
    fsrs_subcontract.recovery_model_q1 AS "sub_recovery_model_q1",
    fsrs_subcontract.recovery_model_q2 AS "sub_recovery_model_q2",
    NULL AS "sub_compensation_q1",
    NULL AS "sub_compensation_q2",
    fsrs_subcontract.principle_place_street AS "sub_place_of_perform_street",

    NOW() AS "created_at",
    NOW() AS "updated_at"

FROM fsrs_procurement
    JOIN fsrs_subcontract AS fsrs_subcontract
        ON fsrs_subcontract.parent_id = fsrs_procurement.id
    LEFT OUTER JOIN base_aw_dap AS bdap
        ON UPPER(TRANSLATE(fsrs_procurement.contract_number, '-', '')) = UPPER(TRANSLATE(bdap.piid, '-', ''))
        AND UPPER(TRANSLATE(fsrs_procurement.idv_reference_number, '-', '')) IS NOT DISTINCT FROM UPPER(TRANSLATE(bdap.parent_award_id, '-', ''))
        AND UPPER(fsrs_procurement.contracting_office_aid) = UPPER(bdap.awarding_sub_tier_agency_c)
    LEFT OUTER JOIN latest_aw_dap AS ldap
        ON UPPER(TRANSLATE(fsrs_procurement.contract_number, '-', '')) = UPPER(TRANSLATE(ldap.piid, '-', ''))
        AND UPPER(TRANSLATE(fsrs_procurement.idv_reference_number, '-', '')) IS NOT DISTINCT FROM UPPER(TRANSLATE(ldap.parent_award_id, '-', ''))
        AND UPPER(fsrs_procurement.contracting_office_aid) = UPPER(ldap.awarding_sub_tier_agency_c)
    LEFT OUTER JOIN country_code AS sub_le_country
        ON (UPPER(fsrs_subcontract.company_address_country) = UPPER(sub_le_country.country_code)
            OR UPPER(fsrs_subcontract.company_address_country) = UPPER(sub_le_country.country_code_2_char))
    LEFT OUTER JOIN country_code AS sub_ppop_country
        ON (UPPER(fsrs_subcontract.principle_place_country) = UPPER(sub_ppop_country.country_code)
            OR UPPER(fsrs_subcontract.principle_place_country) = UPPER(sub_ppop_country.country_code_2_char))
    LEFT OUTER JOIN zips_modified_union AS sub_le_county_code_zip9
        ON (fsrs_subcontract.company_address_country = 'USA'
            AND fsrs_subcontract.company_address_zip = sub_le_county_code_zip9.sub_zip
            AND sub_le_county_code_zip9.type = 'zip9')
    LEFT OUTER JOIN zips_modified_union AS sub_le_county_code_zip5
        ON (fsrs_subcontract.company_address_country = 'USA'
            AND LEFT(fsrs_subcontract.company_address_zip, 5) = sub_le_county_code_zip5.sub_zip
            AND fsrs_subcontract.company_address_state = sub_le_county_code_zip5.state_abbreviation
            AND sub_le_county_code_zip5.type = 'zip5+state')
    LEFT OUTER JOIN county_code AS sub_le_county_name
    	ON (COALESCE(sub_le_county_code_zip9.county_number, sub_le_county_code_zip5.county_number) = sub_le_county_name.county_number
    		AND COALESCE(sub_le_county_code_zip9.state_abbreviation, sub_le_county_code_zip5.state_abbreviation) = sub_le_county_name.state_code)
    LEFT OUTER JOIN zips_modified_union AS sub_ppop_county_code_zip9
        ON (LEFT(fsrs_subcontract.principle_place_country, 2) = 'US'
            AND fsrs_subcontract.principle_place_zip = sub_ppop_county_code_zip9.sub_zip
            AND sub_ppop_county_code_zip9.type = 'zip9')
    LEFT OUTER JOIN zips_modified_union AS sub_ppop_county_code_zip5
        ON (LEFT(fsrs_subcontract.principle_place_country, 2) = 'US'
            AND LEFT(fsrs_subcontract.principle_place_zip, 5) = sub_ppop_county_code_zip5.sub_zip
            AND fsrs_subcontract.principle_place_state = sub_ppop_county_code_zip5.state_abbreviation
            AND sub_ppop_county_code_zip5.type = 'zip5+state')
    LEFT OUTER JOIN county_code AS sub_ppop_county_name
    	ON (COALESCE(sub_ppop_county_code_zip9.county_number, sub_ppop_county_code_zip5.county_number) = sub_ppop_county_name.county_number
    		AND COALESCE(sub_ppop_county_code_zip9.state_abbreviation, sub_ppop_county_code_zip5.state_abbreviation) = sub_ppop_county_name.state_code)
WHERE fsrs_procurement.id {0} {1};
