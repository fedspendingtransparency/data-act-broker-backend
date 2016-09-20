SELECT
    approp.row_number,
    approp.spending_authority_from_of_cpe
FROM appropriation AS approp
    INNER JOIN sf_133 AS sf ON approp.tas = sf.tas
    INNER JOIN submission AS sub ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE approp.submission_id = {}
    AND (sf.line = 1750 OR sf.line = 1850)
    AND sf.amount > 0
    AND (approp.spending_authority_from_of_cpe = 0 OR approp.spending_authority_from_of_cpe IS NULL);