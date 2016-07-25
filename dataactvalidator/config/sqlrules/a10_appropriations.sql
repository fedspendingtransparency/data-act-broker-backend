SELECT
    ap.row_number,
    ap.borrowing_authority_amount_cpe,
    SUM(sf.amount) as total_amount
FROM appropriation as ap
    INNER JOIN sf_133 as sf ON ap.tas = sf.tas
WHERE ap.submission_id = {} AND
    sf.line in (1340, 1440)
GROUP BY ap.row_number, ap.borrowing_authority_amount_cpe
HAVING COALESCE(ap.borrowing_authority_amount_cpe, 0) <> SUM(sf.amount)