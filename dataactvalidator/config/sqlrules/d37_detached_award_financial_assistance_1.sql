-- For new assistance awards (ActionType = A), the CFDA_Number must be active as of the ActionDate.
-- This does not apply to correction records (those with CorrectionLateDeleteIndicator = C).
-- If Archived Date <= Action Date <= Published Date, it passes validation (active).
SELECT
    row_number,
    cfda_number,
    action_type,
    correction_late_delete_ind,
    action_date
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND dafa.action_type = 'A'
    AND ((dafa.correction_late_delete_ind != 'C')
        or (dafa.correction_late_delete_ind is null)
    )
    AND dafa.row_number IN (
        SELECT DISTINCT dafa.row_number
        FROM detached_award_financial_assistance AS dafa
            JOIN cfda_program AS cfda
            ON (CAST(dafa.cfda_number as float) IS NOT DISTINCT FROM CAST(cfda.program_number as float)
            AND (((dafa.action_date < cfda.published_date) AND (cfda.published_date !=''))
                OR ((dafa.action_date > cfda.archived_date) AND (cfda.archived_date != '')))
            )
    )

