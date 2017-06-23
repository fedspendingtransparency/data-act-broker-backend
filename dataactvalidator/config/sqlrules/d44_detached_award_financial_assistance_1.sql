-- When ActionType = A, must be active in the System for Award Management (SAM) on the ActionDate of the award.
-- When ActionType = B, C, or D, DUNS should exist in SAM but need not be active on the ActionDate (i.e., it may be expired).

SELECT
    row_number,
    awardee_or_recipient_uniqu,
    action_type,
    action_date
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND dafa.action_type = 'A'
    AND dafa.row_number NOT IN (
        SELECT DISTINCT dafa.row_number
        FROM detached_award_financial_assistance as dafa
            JOIN executive_compensation AS exec_comp
            ON dafa.awardee_or_recipient_uniqu = exec_comp.awardee_or_recipient_uniqu
            WHERE CAST(dafa.action_date as VarChar) >= CAST(exec_comp.activation_date as VarChar)
    )