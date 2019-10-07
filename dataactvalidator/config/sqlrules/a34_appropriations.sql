-- BudgetAuthorityUnobligatedBalanceBroughtForward_FYB = value for GTAS SF-133 line #2490 from the
-- end of the prior fiscal year.
WITH appropriation_a34_{0} AS
    (SELECT row_number,
        budget_authority_unobligat_fyb,
        tas,
        submission_id
    FROM appropriation
    WHERE submission_id = {0})
SELECT
    approp.row_number,
    approp.budget_authority_unobligat_fyb,
    sf.amount AS "expected_value_GTAS SF133 Line 2490",
    COALESCE(approp.budget_authority_unobligat_fyb, 0) - sf.amount AS "variance"
FROM appropriation_a34_{0} AS approp
    JOIN sf_133 AS sf
        ON approp.tas = sf.tas
    JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
        AND sf.period = 12
        AND sf.fiscal_year = (sub.reporting_fiscal_year - 1)
WHERE sf.line = 2490
    AND COALESCE(approp.budget_authority_unobligat_fyb, 0) <> sf.amount;
