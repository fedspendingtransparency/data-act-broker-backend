WITH aw_dap AS
    (SELECT DISTINCT ON (
            dap.piid,
            dap.parent_award_id,
            dap.awarding_sub_tier_agency_c
        )
        dap.unique_award_key AS unique_award_key,
        dap.piid AS piid,
        dap.idv_type AS idv_type,
        dap.parent_award_id AS parent_award_id,
        dap.award_description as award_description,
        dap.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
        dap.naics_description AS naics_description,
        dap.awarding_agency_code AS awarding_agency_code,
        dap.awarding_agency_name AS awarding_agency_name,
        dap.funding_agency_code AS funding_agency_code,
        dap.funding_agency_name AS funding_agency_name
    FROM detached_award_procurement AS dap
    WHERE EXISTS (SELECT 1 FROM fsrs_procurement WHERE fsrs_procurement.contract_number = dap.piid
        AND fsrs_procurement.idv_reference_number IS NOT DISTINCT FROM dap.parent_award_id
        AND fsrs_procurement.contracting_office_aid = dap.awarding_sub_tier_agency_c)
    ORDER BY dap.piid, dap.parent_award_id, dap.awarding_sub_tier_agency_c, dap.action_date)
UPDATE subaward
SET
    unique_award_key = aw_dap.unique_award_key,
    award_id = fsrs_procurement.contract_number,
    parent_award_id = fsrs_procurement.idv_reference_number,
    award_amount = fsrs_procurement.dollar_obligated,
    action_date = fsrs_procurement.date_signed,
    fy = 'FY' || fy(fsrs_procurement.date_signed),
    awarding_agency_code = aw_dap.awarding_agency_code,
    awarding_agency_name = aw_dap.awarding_agency_name,
    awarding_sub_tier_agency_c = fsrs_procurement.contracting_office_aid,
    awarding_sub_tier_agency_n = fsrs_procurement.contracting_office_aname,
    awarding_office_code = fsrs_procurement.contracting_office_id,
    awarding_office_name = fsrs_procurement.contracting_office_name,
    funding_agency_code = aw_dap.funding_agency_code,
    funding_agency_name = aw_dap.funding_agency_name,
    funding_sub_tier_agency_co = fsrs_procurement.funding_agency_id,
    funding_sub_tier_agency_na = fsrs_procurement.funding_agency_name,
    funding_office_code = fsrs_procurement.funding_office_id,
    funding_office_name = fsrs_procurement.funding_office_name,
    awardee_or_recipient_uniqu = fsrs_procurement.duns,
    awardee_or_recipient_legal = fsrs_procurement.company_name,
    dba_name = fsrs_procurement.dba_name,
    ultimate_parent_unique_ide = fsrs_procurement.parent_duns,
    ultimate_parent_legal_enti = fsrs_procurement.parent_company_name,
    legal_entity_country_code = fsrs_procurement.company_address_country,
    legal_entity_country_name = le_country.country_name,
    legal_entity_address_line1 = fsrs_procurement.company_address_street,
    legal_entity_city_name = fsrs_procurement.company_address_city,
    legal_entity_state_code = fsrs_procurement.company_address_state,
    legal_entity_state_name = fsrs_procurement.company_address_state_name,
    legal_entity_zip = CASE WHEN fsrs_procurement.company_address_country = 'USA'
        THEN fsrs_procurement.company_address_zip
        ELSE NULL
    END,
    legal_entity_congressional = fsrs_procurement.company_address_district,
    legal_entity_foreign_posta = CASE WHEN fsrs_procurement.company_address_country <> 'USA'
        THEN fsrs_procurement.company_address_zip
        ELSE NULL
    END,
    business_types = fsrs_procurement.bus_types,
    place_of_perform_city_name = fsrs_procurement.principle_place_city,
    place_of_perform_state_code = fsrs_procurement.principle_place_state,
    place_of_perform_state_name = fsrs_procurement.principle_place_state_name,
    place_of_performance_zip = fsrs_procurement.principle_place_zip,
    place_of_perform_congressio = fsrs_procurement.principle_place_district,
    place_of_perform_country_co = fsrs_procurement.principle_place_country,
    place_of_perform_country_na = ppop_country.country_name,
    award_description = aw_dap.award_description,
    naics = fsrs_procurement.naics,
    naics_description = aw_dap.naics_description,
    cfda_numbers = NULL,
    cfda_titles = NULL,

    -- File F Subawards
    subaward_type = 'sub-contract',
    subaward_report_year = fsrs_procurement.report_period_year,
    subaward_report_month = fsrs_procurement.report_period_mon,
    subaward_number = fsrs_subcontract.subcontract_num,
    subaward_amount = fsrs_subcontract.subcontract_amount,
    sub_action_date = fsrs_subcontract.subcontract_date,
    sub_awardee_or_recipient_uniqu = fsrs_subcontract.duns,
    sub_awardee_or_recipient_legal = fsrs_subcontract.company_name,
    sub_dba_name = fsrs_subcontract.dba_name,
    sub_ultimate_parent_unique_ide = fsrs_subcontract.parent_duns,
    sub_ultimate_parent_legal_enti = fsrs_subcontract.parent_company_name,
    sub_legal_entity_country_code = fsrs_subcontract.company_address_country,
    sub_legal_entity_country_name = sub_le_country.country_name,
    sub_legal_entity_address_line1 = fsrs_subcontract.company_address_street,
    sub_legal_entity_city_name = fsrs_subcontract.company_address_city,
    sub_legal_entity_state_code = fsrs_subcontract.company_address_state,
    sub_legal_entity_state_name = fsrs_subcontract.company_address_state_name,
    sub_legal_entity_zip = CASE WHEN fsrs_subcontract.company_address_country = 'USA'
        THEN fsrs_subcontract.company_address_zip
        ELSE NULL
    END,
    sub_legal_entity_congressional = fsrs_subcontract.company_address_district,
    sub_legal_entity_foreign_posta = CASE WHEN fsrs_subcontract.company_address_country <> 'USA'
        THEN fsrs_subcontract.company_address_zip
        ELSE NULL
    END,
    sub_business_types = fsrs_subcontract.bus_types,
    sub_place_of_perform_city_name = fsrs_subcontract.principle_place_city,
    sub_place_of_perform_state_code = fsrs_subcontract.principle_place_state,
    sub_place_of_perform_state_name = fsrs_subcontract.principle_place_state_name,
    sub_place_of_performance_zip = fsrs_subcontract.principle_place_zip,
    sub_place_of_perform_congressio = fsrs_subcontract.principle_place_district,
    sub_place_of_perform_country_co = fsrs_subcontract.principle_place_country,
    sub_place_of_perform_country_na = sub_ppop_country.country_name,
    subaward_description = fsrs_subcontract.overall_description,
    sub_high_comp_officer1_full_na = fsrs_subcontract.top_paid_fullname_1,
    sub_high_comp_officer1_amount = fsrs_subcontract.top_paid_amount_1,
    sub_high_comp_officer2_full_na = fsrs_subcontract.top_paid_fullname_2,
    sub_high_comp_officer2_amount = fsrs_subcontract.top_paid_amount_2,
    sub_high_comp_officer3_full_na = fsrs_subcontract.top_paid_fullname_3,
    sub_high_comp_officer3_amount = fsrs_subcontract.top_paid_amount_3,
    sub_high_comp_officer4_full_na = fsrs_subcontract.top_paid_fullname_4,
    sub_high_comp_officer4_amount = fsrs_subcontract.top_paid_amount_4,
    sub_high_comp_officer5_full_na = fsrs_subcontract.top_paid_fullname_5,
    sub_high_comp_officer5_amount = fsrs_subcontract.top_paid_amount_5,

    -- File F Prime Awards
    prime_id = fsrs_procurement.id,
    internal_id = fsrs_procurement.internal_id,
    date_submitted = fsrs_procurement.date_submitted,
    report_type = fsrs_procurement.report_type,
    transaction_type = fsrs_procurement.transaction_type,
    program_title = fsrs_procurement.program_title,
    contract_agency_code = fsrs_procurement.contract_agency_code,
    contract_idv_agency_code = fsrs_procurement.contract_idv_agency_code,
    grant_funding_agency_id = NULL,
    grant_funding_agency_name = NULL,
    federal_agency_name = NULL,
    treasury_symbol = fsrs_procurement.treasury_symbol,
    dunsplus4 = NULL,
    recovery_model_q1 = fsrs_procurement.recovery_model_q1,
    recovery_model_q2 = fsrs_procurement.recovery_model_q2,
    compensation_q1 = NULL,
    compensation_q2 = NULL,
    high_comp_officer1_full_na = fsrs_procurement.top_paid_fullname_1,
    high_comp_officer1_amount = fsrs_procurement.top_paid_amount_1,
    high_comp_officer2_full_na = fsrs_procurement.top_paid_fullname_2,
    high_comp_officer2_amount = fsrs_procurement.top_paid_amount_2,
    high_comp_officer3_full_na = fsrs_procurement.top_paid_fullname_3,
    high_comp_officer3_amount = fsrs_procurement.top_paid_amount_3,
    high_comp_officer4_full_na = fsrs_procurement.top_paid_fullname_4,
    high_comp_officer4_amount = fsrs_procurement.top_paid_amount_4,
    high_comp_officer5_full_na = fsrs_procurement.top_paid_fullname_5,
    high_comp_officer5_amount = fsrs_procurement.top_paid_amount_5,

    -- File F Subawards
    sub_id = fsrs_subcontract.id,
    sub_parent_id = fsrs_subcontract.parent_id,
    sub_federal_agency_id = NULL,
    sub_federal_agency_name = NULL,
    sub_funding_agency_id = fsrs_subcontract.funding_agency_id,
    sub_funding_agency_name = fsrs_subcontract.funding_agency_name,
    sub_funding_office_id = fsrs_subcontract.funding_office_id,
    sub_funding_office_name = fsrs_subcontract.funding_office_name,
    sub_naics = fsrs_subcontract.naics,
    sub_cfda_numbers = NULL,
    sub_dunsplus4 = NULL,
    sub_recovery_subcontract_amt = fsrs_subcontract.recovery_subcontract_amt,
    sub_recovery_model_q1 = fsrs_subcontract.recovery_model_q1,
    sub_recovery_model_q2 = fsrs_subcontract.recovery_model_q2,
    sub_compensation_q1 = NULL,
    sub_compensation_q2 = NULL,
    "updated_at" = now()

FROM fsrs_procurement
    JOIN fsrs_subcontract
        ON fsrs_subcontract.parent_id = fsrs_procurement.id
    LEFT OUTER JOIN aw_dap
        ON (fsrs_procurement.contract_number = aw_dap.piid
        AND fsrs_procurement.idv_reference_number IS NOT DISTINCT FROM aw_dap.parent_award_id
        AND fsrs_procurement.contracting_office_aid = aw_dap.awarding_sub_tier_agency_c)
    LEFT OUTER JOIN country_code AS le_country
        ON fsrs_procurement.company_address_country = le_country.country_code
    LEFT OUTER JOIN country_code AS ppop_country
        ON fsrs_procurement.principle_place_country = ppop_country.country_code
    LEFT OUTER JOIN country_code AS sub_le_country
        ON fsrs_subcontract.company_address_country = sub_le_country.country_code
    LEFT OUTER JOIN country_code AS sub_ppop_country
        ON fsrs_subcontract.principle_place_country = sub_ppop_country.country_code
WHERE subaward.unique_award_key IS NULL
    AND subaward.subaward_type = 'sub-contract'
    AND subaward.prime_id = fsrs_procurement.id
