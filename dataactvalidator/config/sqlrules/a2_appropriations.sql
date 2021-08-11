-- TotalBudgetaryResources_CPE = BudgetAuthorityAppropriatedAmount_CPE +
-- BudgetAuthorityUnobligatedBalanceBroughtForward_FYB + AdjustmentsToUnobligatedBalanceBroughtForward_CPE +
-- OtherBudgetaryResourcesAmount_CPE + SF 133 Line 1902
SELECT
    row_number,
    total_budgetary_resources_cpe,
    budget_authority_appropria_cpe,
    budget_authority_unobligat_fyb,
    adjustments_to_unobligated_cpe,
    other_budgetary_resources_cpe,
    COALESCE(total_budgetary_resources_cpe, 0) - (COALESCE(budget_authority_appropria_cpe, 0) +
                                                  COALESCE(budget_authority_unobligat_fyb, 0) +
                                                  COALESCE(adjustments_to_unobligated_cpe, 0) +
                                                  COALESCE(other_budgetary_resources_cpe, 0)) AS "difference",
    approp.display_tas AS "uniqueid_TAS"
FROM appropriation AS approp
    INNER JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
    LEFT OUTER JOIN sf_133 AS sf
        ON approp.tas = sf.tas
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
        AND sf.line = 1902
WHERE approp.submission_id = {0}
    AND COALESCE(total_budgetary_resources_cpe, 0) <>
        COALESCE(budget_authority_appropria_cpe, 0) +
        COALESCE(budget_authority_unobligat_fyb, 0) +
        COALESCE(adjustments_to_unobligated_cpe, 0) +
        COALESCE(other_budgetary_resources_cpe, 0) +
        COALESCE(sf.amount, 0);
