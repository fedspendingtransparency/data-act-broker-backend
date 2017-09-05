-- For other assistance awards (ActionType = B, C, or D), the CFDA_Number need NOT be active as of the ActionDate.
-- This does not apply to correction records (those with CorrectionLateDeleteIndicator = C).
-- Should not be active (action_date <= archived_date and when archived date exists)
-- If the ActionDate is < published_date, should trigger a warning.
WITH detached_award_financial_assistance_fabs37_2_{0} AS
    (SELECT submission_id,
        row_number,
        cfda_number,
        action_type,
        correction_late_delete_ind,
        action_date
    FROM detached_award_financial_assistance
    WHERE submission_id = {0})
SELECT
    row_number,
    cfda_number,
    action_type,
    correction_late_delete_ind,
    action_date
FROM detached_award_financial_assistance_fabs37_2_{0} AS dafa
WHERE dafa.action_type IN ('B', 'C', 'D')
    AND ((dafa.correction_late_delete_ind != 'C')
        or (dafa.correction_late_delete_ind is null)
    )
    AND dafa.row_number IN (
        SELECT DISTINCT sub_dafa.row_number
        FROM detached_award_financial_assistance_fabs37_2_{0} AS sub_dafa
            JOIN cfda_program AS cfda
            ON (sub_dafa.cfda_number = to_char(cfda.program_number, 'FM00.000')
            AND ((sub_dafa.action_date <= cfda.published_date)
                 OR ((sub_dafa.action_date >= cfda.archived_date)
                     AND (cfda.archived_date != ''))
            ))
    )