SELECT DISTINCT approp.row_number,
    approp.allocation_transfer_agency,
	approp.agency_identifier,
	approp.beginning_period_of_availa,
	approp.ending_period_of_availabil,
	approp.availability_type_code,
	approp.main_account_code,
	approp.sub_account_code
FROM appropriation AS approp
	JOIN submission AS sub
	    ON approp.submission_id = sub.submission_id
WHERE approp.submission_id = {0}
	AND NOT EXISTS (
		SELECT 1
		FROM sf_133 AS sf
        WHERE approp.tas IS NOT DISTINCT FROM sf.tas
            AND sf.period = sub.reporting_fiscal_period
	        AND sf.fiscal_year = sub.reporting_fiscal_year
	)
    AND (
        COALESCE(approp.adjustments_to_unobligated_cpe,0) <> 0
        OR COALESCE(approp.budget_authority_appropria_cpe,0) <> 0
        OR COALESCE(approp.borrowing_authority_amount_cpe,0) <> 0
        OR COALESCE(approp.contract_authority_amount_cpe,0) <> 0
        OR COALESCE(approp.spending_authority_from_of_cpe,0) <> 0
        OR COALESCE(approp.other_budgetary_resources_cpe,0) <> 0
        OR COALESCE(approp.budget_authority_available_cpe,0) <> 0
        OR COALESCE(approp.gross_outlay_amount_by_tas_cpe,0) <> 0
        OR COALESCE(approp.obligations_incurred_total_cpe,0) <> 0
        OR COALESCE(approp.deobligations_recoveries_r_cpe,0) <> 0
        OR COALESCE(approp.unobligated_balance_cpe,0) <> 0
        OR COALESCE(approp.status_of_budgetary_resour_cpe,0) <> 0
    );