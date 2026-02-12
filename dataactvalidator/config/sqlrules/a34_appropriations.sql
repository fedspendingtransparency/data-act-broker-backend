-- BudgetAuthorityUnobligatedBalanceBroughtForward_FYB = value for GTAS SF-133 line #2490 from the
-- end of the prior fiscal year.
WITH appropriation_a34_{0} AS
    (SELECT row_number,
        budget_authority_unobligat_fyb,
        display_tas,
        submission_id
    FROM appropriation
    WHERE submission_id = {0})
SELECT
    approp.row_number,
    approp.budget_authority_unobligat_fyb,
    SUM(COALESCE(sf.amount, 0)) AS "expected_value_GTAS SF133 Line 2490",
    COALESCE(approp.budget_authority_unobligat_fyb, 0) - SUM(COALESCE(sf.amount, 0)) AS "difference",
    approp.display_tas AS "uniqueid_TAS"
FROM appropriation_a34_{0} AS approp
    JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
    LEFT OUTER JOIN sf_133 AS sf
        ON approp.display_tas = sf.display_tas
        AND sf.period = 12
        AND sf.fiscal_year = (sub.reporting_fiscal_year - 1)
        AND sf.line = 2490
GROUP BY approp.row_number,
    approp.budget_authority_unobligat_fyb,
    approp.display_tas
HAVING COALESCE(approp.budget_authority_unobligat_fyb, 0) <> SUM(COALESCE(sf.amount, 0));
