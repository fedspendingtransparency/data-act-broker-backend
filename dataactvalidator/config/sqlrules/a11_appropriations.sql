-- SpendingAuthorityfromOffsettingCollectionsAmountTotal_CPE = CPE aggregate value for GTAS SF-133 line #1750 + #1850
WITH appropriation_a11_{0} AS
    (SELECT submission_id,
        row_number,
        spending_authority_from_of_cpe,
        tas
    FROM appropriation
    WHERE submission_id = {0})
SELECT
    approp.row_number,
    approp.spending_authority_from_of_cpe,
    SUM(sf.amount) AS "expected_value_SUM of GTAS SF133 Lines 1750, 1850",
    COALESCE(approp.spending_authority_from_of_cpe, 0) - SUM(sf.amount) AS "variance"
FROM appropriation_a11_{0} AS approp
    INNER JOIN sf_133 AS sf
        ON approp.tas = sf.tas
    INNER JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE sf.line IN (1750, 1850)
GROUP BY approp.row_number,
    approp.spending_authority_from_of_cpe
HAVING COALESCE(approp.spending_authority_from_of_cpe, 0) <> SUM(sf.amount);
