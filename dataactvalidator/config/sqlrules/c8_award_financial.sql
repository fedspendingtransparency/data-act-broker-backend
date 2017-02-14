SELECT af.row_number, af.fain, af.uri
FROM award_financial AS af
WHERE af.submission_id = {0}
    AND NOT EXISTS (
        SELECT cgac_code
        FROM cgac
        WHERE cgac_code = af.allocation_transfer_agency
	) AND (af.fain IS NOT NULL
            OR af.uri IS NOT NULL
	) AND (af.row_number NOT IN (
            SELECT af.row_number
            FROM award_financial AS af
                JOIN award_financial_assistance AS afa
                    ON af.submission_id = afa.submission_id
                        AND af.fain = afa.fain
            WHERE af.submission_id = {0}
        ) AND af.row_number NOT IN (
            SELECT af.row_number
            FROM award_financial AS af
                JOIN award_financial_assistance AS afa
                    ON af.submission_id = afa.submission_id
                        AND af.uri = afa.uri
            WHERE af.submission_id = {0}
        )
    );