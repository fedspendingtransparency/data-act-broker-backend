SELECT
    approp.row_number,
    approp.borrowing_authority_amount_cpe,
    SUM(sf.amount) as total_amount
FROM appropriation as approp
    INNER JOIN sf_133 as sf ON approp.tas = sf.tas
WHERE approp.submission_id = {} AND
    sf.line in (1340, 1440)
GROUP BY approp.row_number, approp.borrowing_authority_amount_cpe
HAVING COALESCE(approp.borrowing_authority_amount_cpe, 0) <> SUM(sf.amount)