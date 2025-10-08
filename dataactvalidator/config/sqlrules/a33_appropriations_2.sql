-- Verify that all of the submitted data (from File A (appropriation)) has an associated GTAS
-- Each TAS reported in File A should be reported in GTAS for SF-133, with the exception of Financing Accounts, or
-- when all monetary amounts are zero for the TAS.
WITH appropriation_a33_2_{0} AS 
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
        AND COALESCE(UPPER(tas_lookup.financial_indicator2), '') <> 'F')
SELECT DISTINCT
    approp.row_number,
    approp.allocation_transfer_agency,
    approp.agency_identifier,
    approp.beginning_period_of_availa,
    approp.ending_period_of_availabil,
    approp.availability_type_code,
    approp.main_account_code,
    approp.sub_account_code,
    approp.display_tas AS "uniqueid_TAS"
FROM appropriation_a33_2_{0} AS approp
    JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
    AND NOT EXISTS (
        SELECT 1
        FROM sf_133 AS sf
        WHERE approp.display_tas = sf.display_tas
            AND sf.period = sub.reporting_fiscal_period
            AND sf.fiscal_year = sub.reporting_fiscal_year
    )
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
    );
