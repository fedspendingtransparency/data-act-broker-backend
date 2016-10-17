SELECT DISTINCT af.row_number, af.fain, af.uri
FROM award_financial AS af
WHERE af.submission_id = {0}
    AND NOT EXISTS (
        SELECT cgac_code
        FROM cgac
        WHERE cgac_code = af.allocation_transfer_agency
	)
	AND (
		af.row_number NOT IN (
            SELECT af.row_number
            FROM award_financial AS af
                JOIN award_financial_assistance AS afa
                    ON (af.submission_id = afa.submission_id
                        AND (af.fain IS NOT DISTINCT FROM afa.fain
                            AND af.uri IS NOT DISTINCT FROM afa.uri)
                    )
            WHERE af.submission_id = {0}
        ) OR (
            af.fain IS DISTINCT FROM NULL
            AND af.uri IS DISTINCT FROM NULL
        )
	);