SELECT
    approp.row_number,
    approp.unobligated_balance_cpe,
    sf.amount
FROM appropriation as approp
    INNER JOIN sf_133 as sf ON (approp.tas = sf.tas)
WHERE approp.submission_id = {} AND
    sf.line = 2490 AND
    COALESCE(approp.unobligated_balance_cpe, 0) <> sf.amount