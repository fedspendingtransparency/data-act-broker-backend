-- TotalBudgetaryResources_CPE = BudgetAuthorityAppropriatedAmount_CPE +
-- BudgetAuthorityUnobligatedBalanceBroughtForward_FYB + AdjustmentsToUnobligatedBalanceBroughtForward_CPE +
-- OtherBudgetaryResourcesAmount_CPE + SF 133 Line 1902
WITH appropriation_a2_{0} AS
    (SELECT submission_id,
        row_number,
        total_budgetary_resources_cpe,
        budget_authority_appropria_cpe,
        budget_authority_unobligat_fyb,
        adjustments_to_unobligated_cpe,
        other_budgetary_resources_cpe,
        tas,
        display_tas
    FROM appropriation
    WHERE submission_id = {0})
SELECT
    approp.row_number,
    approp.total_budgetary_resources_cpe,
    approp.budget_authority_appropria_cpe,
    approp.budget_authority_unobligat_fyb,
    approp.adjustments_to_unobligated_cpe,
    approp.other_budgetary_resources_cpe,
    COALESCE(SUM(sf.amount), 0) AS "expected_value_GTAS SF133 Line 1902",
    COALESCE(approp.total_budgetary_resources_cpe, 0) - (COALESCE(approp.budget_authority_appropria_cpe, 0) +
                                                         COALESCE(approp.budget_authority_unobligat_fyb, 0) +
                                                         COALESCE(approp.adjustments_to_unobligated_cpe, 0) +
                                                         COALESCE(approp.other_budgetary_resources_cpe, 0) +
                                                         COALESCE(SUM(sf.amount), 0)) AS "difference",
    approp.display_tas AS "uniqueid_TAS"
FROM appropriation_a2_{0} AS approp
    INNER JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
    LEFT OUTER JOIN sf_133 AS sf
        ON approp.tas = sf.tas
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
        AND sf.line = 1902
GROUP BY approp.row_number,
    approp.total_budgetary_resources_cpe,
    approp.budget_authority_appropria_cpe,
    approp.budget_authority_unobligat_fyb,
    approp.adjustments_to_unobligated_cpe,
    approp.other_budgetary_resources_cpe,
    approp.display_tas
HAVING COALESCE(approp.total_budgetary_resources_cpe, 0) <>
    COALESCE(approp.budget_authority_appropria_cpe, 0) +
    COALESCE(approp.budget_authority_unobligat_fyb, 0) +
    COALESCE(approp.adjustments_to_unobligated_cpe, 0) +
    COALESCE(approp.other_budgetary_resources_cpe, 0) +
    COALESCE(SUM(sf.amount), 0);
