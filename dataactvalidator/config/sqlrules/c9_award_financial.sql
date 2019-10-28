-- Unique FAIN and/or URI from file D2 should exist in file C, except for:
-- 1) Loans (AssistanceType = 08 or 09) with OriginalLoanSubsidyCost <= 0 in D2; or
-- 2) Non-Loans with FederalActionObligation = 0 in D2.
-- FAIN may be null for aggregated records. URI may be null for non-aggregated records.
WITH award_financial_assistance_c9_{0} AS
    (SELECT submission_id,
        row_number,
        federal_action_obligation,
        original_loan_subsidy_cost,
        fain,
        uri,
        assistance_type
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
    afa.row_number AS "source_row_number",
    afa.fain AS "source_value_fain",
    afa.uri AS "source_value_uri",
    afa.fain AS "uniqueid_FAIN",
    afa.uri AS "uniqueid_URI"
FROM award_financial_assistance_c9_{0} AS afa
WHERE ((afa.assistance_type NOT IN ('07', '08')
            AND COALESCE(afa.federal_action_obligation, 0) <> 0
        )
        OR (afa.assistance_type IN ('07', '08')
            AND COALESCE(CAST(afa.original_loan_subsidy_cost AS NUMERIC), 0) > 0
        )
    )
    AND (afa.fain IS NOT NULL
        OR afa.uri IS NOT NULL
    )
    AND NOT EXISTS (
        SELECT 1
        FROM award_financial_c9_{0} AS af
        WHERE UPPER(COALESCE(afa.fain, '')) = UPPER(COALESCE(af.fain, ''))
            AND UPPER(COALESCE(afa.uri, '')) = UPPER(COALESCE(af.uri, ''))
    );
