SELECT
    ap.row_number,
    ap.gross_outlay_amount_by_tas_cpe,
    sf.amount
FROM ap.appropriation as ap
    INNER JOIN sf_133 as sf ON (ap.tas = sf.tas)
WHERE submission_id = {} AND
    sf.line = 3020 AND
    COALESCE(ap.gross_outlay_amount_by_tas_cpe, 0) <> sf.amount