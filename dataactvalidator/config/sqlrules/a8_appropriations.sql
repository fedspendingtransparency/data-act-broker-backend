SELECT
    approp.row_number,
    approp.budget_authority_appropria_cpe,
    SUM(sf.amount) as total_amount
FROM appropriation as approp
    INNER JOIN sf_133 as sf ON approp.tas = sf.tas
WHERE approp.submission_id = {} AND
    sf.line in (1160, 1180, 1260, 1280)
GROUP BY approp.row_number, approp.budget_authority_appropria_cpe
HAVING COALESCE(approp.budget_authority_appropria_cpe, 0) <> SUM(sf.amount)