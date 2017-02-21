WITH appropriation_a14_{0} AS 
	(SELECT submission_id,
		row_number,
		gross_outlay_amount_by_tas_cpe,
		tas
	FROM appropriation
    WHERE submission_id = {0})
SELECT
    approp.row_number,
    approp.gross_outlay_amount_by_tas_cpe,
    sf.amount as sf_133_amount
FROM appropriation_a14_{0} as approp
    INNER JOIN sf_133 as sf ON approp.tas = sf.tas
    INNER JOIN submission as sub ON approp.submission_id = sub.submission_id AND
        sf.period = sub.reporting_fiscal_period AND
        sf.fiscal_year = sub.reporting_fiscal_year
WHERE sf.line = 3020 AND
    approp.gross_outlay_amount_by_tas_cpe <> sf.amount