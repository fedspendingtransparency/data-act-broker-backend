SELECT afa.row_number, afa.fain, afa.uri
FROM award_financial_assistance AS afa
WHERE afa.submission_id = {0}
    AND (COALESCE(CAST(afa.federal_action_obligation as numeric),0) <> 0
        OR COALESCE(CAST(afa.original_loan_subsidy_cost as numeric),0) <> 0
    ) AND (afa.fain IS NOT NULL
            OR afa.uri IS NOT NULL
	) AND (afa.row_number NOT IN (
            SELECT afa.row_number
            FROM award_financial_assistance AS afa
                JOIN award_financial AS af
                    ON afa.submission_id = af.submission_id
                        AND afa.fain = af.fain
            WHERE afa.submission_id = {0}
        ) AND afa.row_number NOT IN (
            SELECT afa.row_number
            FROM award_financial_assistance AS afa
                JOIN award_financial AS af
                    ON afa.submission_id = af.submission_id
                        AND afa.uri = af.uri
            WHERE afa.submission_id = {0}
        )
    );