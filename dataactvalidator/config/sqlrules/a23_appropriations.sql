-- StatusOfBudgetaryResourcesTotal_CPE = CPE value for GTAS SF-133 line #2500
SELECT
    approp.row_number,
    approp.status_of_budgetary_resour_cpe,
    SUM(sf.amount) AS "expected_value_GTAS SF133 Line 2500",
    approp.status_of_budgetary_resour_cpe - SUM(sf.amount) AS "difference",
    approp.display_tas AS "uniqueid_TAS"
FROM appropriation AS approp
    INNER JOIN sf_133 AS sf
        ON approp.display_tas = sf.display_tas
    INNER JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE approp.submission_id = {0}
    AND sf.line = 2500
GROUP BY approp.row_number,
    approp.status_of_budgetary_resour_cpe,
    approp.display_tas
HAVING approp.status_of_budgetary_resour_cpe <> SUM(sf.amount);
