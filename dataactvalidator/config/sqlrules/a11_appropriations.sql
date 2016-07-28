SELECT
    approp.row_number,
    approp.spending_authority_from_of_cpe,
    SUM(sf.amount) as total_amount
FROM appropriation as approp
    INNER JOIN sf_133 as sf ON approp.tas = sf.tas
WHERE approp.submission_id = {} AND
    sf.line in (1750, 1850)
GROUP BY approp.row_number, approp.spending_authority_from_of_cpe
HAVING COALESCE(approp.spending_authority_from_of_cpe, 0) <> SUM(sf.amount)