SELECT DISTINCT
    approp.row_number,
    approp.borrowing_authority_amount_cpe,
    string_agg(CAST(sf.line AS varchar), ', ') AS lines,
    string_agg(CAST(sf.amount AS varchar), ', ') AS amounts
FROM appropriation AS approp
    INNER JOIN sf_133 AS sf ON approp.tas = sf.tas
    INNER JOIN submission AS sub ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE approp.submission_id = {}
    AND sf.line IN (1340, 1440)
    AND sf.amount > 0
    AND COALESCE(approp.borrowing_authority_amount_cpe,0) = 0
GROUP BY
    approp.row_number,
    approp.borrowing_authority_amount_cpe;