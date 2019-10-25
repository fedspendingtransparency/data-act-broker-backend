-- For new assistance awards (ActionType = A), the CFDA_Number must be active as of the ActionDate.
-- This does not apply to correction records (those with CorrectionDeleteIndicator = C).
-- If publish_date <= action_date <= archived_date, it passes validation (active).
WITH detached_award_financial_assistance_fabs37_1_{0} AS
    (SELECT submission_id,
        row_number,
        cfda_number,
        action_type,
        correction_delete_indicatr,
        action_date,
        afa_generated_unique
    FROM detached_award_financial_assistance
    WHERE submission_id = {0})
SELECT
    row_number,
    cfda_number,
    action_type,
    correction_delete_indicatr,
    action_date,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance_fabs37_1_{0} AS dafa
WHERE UPPER(dafa.action_type) = 'A'
    AND UPPER(COALESCE(correction_delete_indicatr, '')) NOT IN ('C', 'D')
    AND dafa.row_number NOT IN (
        SELECT DISTINCT sub_dafa.row_number
        FROM detached_award_financial_assistance_fabs37_1_{0} AS sub_dafa
            JOIN cfda_program AS cfda
                ON sub_dafa.cfda_number = to_char(cfda.program_number, 'FM00.000')
                AND ((cfda.published_date <= sub_dafa.action_date
                        AND cfda.archived_date = ''
                    )
                    OR (sub_dafa.action_date <= cfda.archived_date
                        AND cfda.archived_date <> ''
                    )
                )
    );
