-- TotalBudgetaryResources_CPE = CPE value for GTAS SF-133 line #1910
WITH appropriation_a6_{0} AS
    (SELECT submission_id,
        row_number,
        total_budgetary_resources_cpe,
        tas
    FROM appropriation
    WHERE submission_id = {0})
SELECT
    approp.row_number,
    approp.total_budgetary_resources_cpe,
    sf.amount AS "expected_value_GTAS SF133 Line 1910",
    approp.total_budgetary_resources_cpe - sf.amount AS "difference",
    approp.tas AS "uniqueid_TAS"
FROM appropriation_a6_{0} AS approp
    INNER JOIN sf_133 AS sf
        ON approp.tas = sf.tas
    INNER JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE sf.line = 1910
    AND approp.total_budgetary_resources_cpe <> sf.amount;
