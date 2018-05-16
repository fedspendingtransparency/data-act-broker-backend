-- StatusOfBudgetaryResourcesTotal_CPE = CPE value for GTAS SF-133 line #2500
SELECT
    approp.row_number,
    approp.status_of_budgetary_resour_cpe,
    sf.amount AS sf_133_amount
FROM appropriation AS approp
    INNER JOIN sf_133 AS sf
        ON approp.tas = sf.tas
    INNER JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE approp.submission_id = {0}
    AND sf.line = 2500
    AND approp.status_of_budgetary_resour_cpe <> sf.amount;
