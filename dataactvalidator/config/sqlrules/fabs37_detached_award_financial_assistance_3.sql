-- For new assistance awards (ActionType = A), the CFDA_Number must have been registered
WITH detached_award_financial_assistance_fabs37_1_{0} AS
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
FROM detached_award_financial_assistance_fabs37_1_{0} AS dafa
WHERE
    -- should always fail if the ID isn't in the history at all
    dafa.row_number NOT IN (
        SELECT DISTINCT sub_dafa.row_number
        FROM detached_award_financial_assistance_fabs37_1_{0} AS sub_dafa
            JOIN cfda_program AS cfda
            ON sub_dafa.cfda_number = to_char(cfda.program_number, 'FM00.000'))