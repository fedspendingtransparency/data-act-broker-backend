SELECT DISTINCT af.row_number, af.fain, af.uri
FROM award_financial AS af
	JOIN award_financial_assistance AS afa
		ON (af.submission_id = afa.submission_id
			AND (
				af.fain IS DISTINCT FROM afa.fain
				OR af.uri IS DISTINCT FROM afa.uri
			)
		)
WHERE af.submission_id = {0}
	AND NOT EXISTS (
        SELECT cgac_code
        FROM cgac
        WHERE cgac_code = af.allocation_transfer_agency
	);