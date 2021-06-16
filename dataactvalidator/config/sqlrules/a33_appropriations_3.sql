-- Verify that all of the submitted data (from File A (appropriation)) has an ATA that matches the CGAC of the
-- submission, with the exception of Financing Accounts, null ATAs, or when all monetary amounts are zero for the TAS.
WITH appropriation_a33_3_{0} AS
    (SELECT row_number,
        approp.allocation_transfer_agency,
        approp.agency_identifier,
        approp.beginning_period_of_availa,
        approp.ending_period_of_availabil,
        approp.availability_type_code,
        approp.main_account_code,
        approp.sub_account_code,
        approp.submission_id,
        approp.display_tas,
        approp.adjustments_to_unobligated_cpe,
        approp.budget_authority_appropria_cpe,
        approp.borrowing_authority_amount_cpe,
        approp.contract_authority_amount_cpe,
        approp.spending_authority_from_of_cpe,
        approp.other_budgetary_resources_cpe,
        approp.total_budgetary_resources_cpe,
        approp.gross_outlay_amount_by_tas_cpe,
        approp.obligations_incurred_total_cpe,
        approp.deobligations_recoveries_r_cpe,
        approp.unobligated_balance_cpe,
        approp.status_of_budgetary_resour_cpe
    FROM appropriation as approp
    LEFT JOIN tas_lookup
        ON tas_lookup.account_num = approp.account_num
    WHERE submission_id = {0}
        -- In case a file a submission contains a financial account
        AND COALESCE(UPPER(tas_lookup.financial_indicator2), '') <> 'F'),
cgac_frec_combo_a33_3_{0} AS
	(SELECT cgac.cgac_code, frec.frec_code
	FROM cgac
	LEFT JOIN frec
		ON frec.cgac_id = cgac.cgac_id),
cgac_sub_{0} AS
    (SELECT sub.submission_id, COALESCE(frec_result.cgac_code, cgac_result.cgac_code) AS cgac_code
    FROM submission AS sub
    LEFT JOIN cgac_frec_combo_a33_3_{0} AS cgac_result
            ON sub.cgac_code = cgac_result.cgac_code
    LEFT JOIN cgac_frec_combo_a33_3_{0} AS frec_result
            ON sub.frec_code = frec_result.frec_code
    WHERE submission_id = {0})
SELECT DISTINCT
    approp.row_number,
    approp.allocation_transfer_agency,
    approp.agency_identifier,
    approp.beginning_period_of_availa,
    approp.ending_period_of_availabil,
    approp.availability_type_code,
    approp.main_account_code,
    approp.sub_account_code,
    approp.display_tas AS "uniqueid_TAS",
    sub.cgac_code AS "expected_value_ATA"
FROM appropriation_a33_3_{0} AS approp
    JOIN cgac_sub_{0} AS sub
        ON approp.submission_id = sub.submission_id
    AND (COALESCE(approp.adjustments_to_unobligated_cpe, 0) <> 0
        OR COALESCE(approp.budget_authority_appropria_cpe, 0) <> 0
        OR COALESCE(approp.borrowing_authority_amount_cpe, 0) <> 0
        OR COALESCE(approp.contract_authority_amount_cpe, 0) <> 0
        OR COALESCE(approp.spending_authority_from_of_cpe, 0) <> 0
        OR COALESCE(approp.other_budgetary_resources_cpe, 0) <> 0
        OR COALESCE(approp.total_budgetary_resources_cpe, 0) <> 0
        OR COALESCE(approp.gross_outlay_amount_by_tas_cpe, 0) <> 0
        OR COALESCE(approp.obligations_incurred_total_cpe, 0) <> 0
        OR COALESCE(approp.deobligations_recoveries_r_cpe, 0) <> 0
        OR COALESCE(approp.unobligated_balance_cpe, 0) <> 0
        OR COALESCE(approp.status_of_budgetary_resour_cpe, 0) <> 0
    )
    AND COALESCE(approp.allocation_transfer_agency, '') <> ''
    AND CASE WHEN sub.cgac_code <> '097'
        THEN approp.allocation_transfer_agency <> sub.cgac_code
        ELSE approp.allocation_transfer_agency NOT IN ('017', '021', '057', '097')
    END;
