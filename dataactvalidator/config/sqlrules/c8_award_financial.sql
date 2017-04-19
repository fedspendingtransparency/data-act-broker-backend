WITH award_financial_c8_{0} AS
    (SELECT submission_id,
        row_number,
        allocation_transfer_agency,
        transaction_obligated_amou,
        fain,
        uri
    FROM award_financial
    WHERE submission_id = {0}),
award_financial_assistance_c8_{0} AS
    (SELECT submission_id,
        row_number,
        fain,
        uri
    FROM award_financial_assistance
    WHERE submission_id = {0})
SELECT
    af.row_number,
    af.fain,
    af.uri
FROM award_financial_c8_{0} AS af
WHERE af.transaction_obligated_amou IS NOT NULL
    AND NOT EXISTS (
        SELECT cgac_code
        FROM cgac
        WHERE cgac_code = af.allocation_transfer_agency
	) AND (af.fain IS NOT NULL
            OR af.uri IS NOT NULL
	) AND (af.row_number NOT IN (
            SELECT DISTINCT af.row_number
            FROM award_financial_c8_{0} AS af
                JOIN award_financial_assistance_c8_{0} AS afa
                    ON af.submission_id = afa.submission_id
                        AND af.fain = afa.fain
        ) AND af.row_number NOT IN (
            SELECT DISTINCT af.row_number
            FROM award_financial_c8_{0} AS af
                JOIN award_financial_assistance_c8_{0} AS afa
                    ON af.submission_id = afa.submission_id
                        AND af.uri = afa.uri
        )
    );