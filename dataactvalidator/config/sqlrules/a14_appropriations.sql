SELECT
    approp.row_number,
    approp.gross_outlay_amount_by_tas_cpe,
    sf.amount
FROM approp.appropriation as approp
    INNER JOIN sf_133 as sf ON (approp.tas = sf.tas)
WHERE submission_id = {} AND
    sf.line = 3020 AND
    COALESCE(approp.gross_outlay_amount_by_tas_cpe, 0) <> sf.amount