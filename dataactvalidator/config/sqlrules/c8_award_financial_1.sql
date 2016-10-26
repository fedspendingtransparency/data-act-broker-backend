SELECT af.row_number, af.fain, af.uri
FROM award_financial AS af
WHERE af.submission_id = {0}
    AND NOT EXISTS (
        SELECT cgac_code
        FROM cgac
        WHERE cgac_code = af.allocation_transfer_agency
	)
	AND af.fain IS DISTINCT FROM NULL
    AND af.uri IS DISTINCT FROM NULL;