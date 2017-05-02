-- For new assistance awards (ActionType = B, C, or D), the CFDA_Number need not be active as of the ActionDate.
-- This does not apply to correction records (those with CorrectionLateDeleteIndicator = C).
-- Active date: Archived Date >= Action Date >= Published Date.
SELECT
    row_number,
    cfda_number,
    action_type,
    correction_late_delete_ind,
    action_date
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND dafa.action_type IN ('B', 'C', 'D')
    AND ((dafa.correction_late_delete_ind != 'C')
        or (dafa.correction_late_delete_ind is null)
    )
    AND dafa.row_number IN (
        SELECT DISTINCT dafa.row_number
        FROM detached_award_financial_assistance AS dafa
            JOIN cfda_program AS cfda
            ON (CAST(dafa.cfda_number as float) IS NOT DISTINCT FROM CAST(cfda.program_number as float)
            AND (((dafa.action_date >= cfda.published_date) AND (cfda.published_date !=''))
                OR ((dafa.action_date <= cfda.archived_date) AND (cfda.archived_date !='')))
            )
    )

