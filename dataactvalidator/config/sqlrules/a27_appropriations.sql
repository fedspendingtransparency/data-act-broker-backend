SELECT DISTINCT
    approp.row_number,
    approp.spending_authority_from_of_cpe,
    string_agg(CAST(sf.line AS varchar), ', ') AS lines,
    string_agg(CAST(sf.amount AS varchar), ', ') AS amounts
FROM appropriation AS approp
    INNER JOIN sf_133 AS sf ON approp.tas = sf.tas
    INNER JOIN submission AS sub ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE approp.submission_id = {}
    AND sf.line IN (1750, 1850)
    AND sf.amount > 0
    AND COALESCE(approp.spending_authority_from_of_cpe,0) = 0
GROUP BY
    approp.row_number,
    approp.spending_authority_from_of_cpe;