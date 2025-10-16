-- BudgetAuthorityUnobligatedBalanceBroughtForward_FYB = value for GTAS SF-133 line #1000
WITH appropriation_a7_{0} AS
    (SELECT submission_id,
        row_number,
        budget_authority_unobligat_fyb,
        display_tas
    FROM appropriation
    WHERE submission_id = {0})
SELECT
    approp.row_number,
    approp.budget_authority_unobligat_fyb,
    SUM(sf.amount) AS "expected_value_GTAS SF133 Line 1000",
    COALESCE(approp.budget_authority_unobligat_fyb, 0) - SUM(sf.amount) AS "difference",
    approp.display_tas AS "uniqueid_TAS"
FROM appropriation_a7_{0} AS approp
    INNER JOIN sf_133 AS sf
        ON approp.display_tas = sf.display_tas
    INNER JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE sf.line = 1000
GROUP BY approp.row_number,
    approp.budget_authority_unobligat_fyb,
    approp.display_tas
HAVING COALESCE(approp.budget_authority_unobligat_fyb, 0) <> SUM(sf.amount);
