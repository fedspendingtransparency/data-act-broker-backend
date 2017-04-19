WITH award_financial_assistance_c9_{0} AS
    (SELECT submission_id,
        row_number,
        federal_action_obligation,
        original_loan_subsidy_cost,
        fain,
        uri
    FROM award_financial_assistance
    WHERE submission_id = {0}),
award_financial_c9_{0} AS
    (SELECT submission_id,
        row_number,
        fain,
        uri
    FROM award_financial
    WHERE submission_id = {0})
SELECT
    afa.row_number,
    afa.fain,
    afa.uri
FROM award_financial_assistance_c9_{0} AS afa
WHERE (COALESCE(afa.federal_action_obligation, 0) <> 0
        OR COALESCE(CAST(afa.original_loan_subsidy_cost as numeric),0) <> 0
    ) AND (afa.fain IS NOT NULL
            OR afa.uri IS NOT NULL
    ) AND afa.row_number NOT IN (
            SELECT DISTINCT afa.row_number
            FROM award_financial_assistance_c9_{0} AS afa
                JOIN award_financial_c9_{0} AS af
                    ON afa.submission_id = af.submission_id
                        AND afa.fain IS NOT DISTINCT FROM af.fain
                        AND afa.uri IS NOT DISTINCT FROM af.uri
        );