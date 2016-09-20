SELECT
    approp.row_number,
    approp.contract_authority_amount_cpe
FROM appropriation AS approp
    INNER JOIN sf_133 AS sf ON approp.tas = sf.tas
    INNER JOIN submission AS sub ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE approp.submission_id = {}
    AND (sf.line = 1540 OR sf.line = 1640)
    AND sf.amount > 0
    AND (approp.contract_authority_amount_cpe = 0 OR approp.contract_authority_amount_cpe IS NULL);