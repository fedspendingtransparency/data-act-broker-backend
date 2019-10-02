-- BudgetAuthorityUnobligatedBalanceBroughtForward_FYB = value for GTAS SF-133 line #1000
WITH appropriation_a7_{0} AS
    (SELECT submission_id,
        row_number,
        budget_authority_unobligat_fyb,
        tas
    FROM appropriation
    WHERE submission_id = {0})
SELECT
    approp.row_number,
    approp.budget_authority_unobligat_fyb,
    sf.amount AS "expected_value_GTAS SF133 Line 1000"
FROM appropriation_a7_{0} AS approp
    INNER JOIN sf_133 AS sf
        ON approp.tas = sf.tas
    INNER JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE sf.line = 1000
    AND COALESCE(approp.budget_authority_unobligat_fyb, 0) <> sf.amount;
