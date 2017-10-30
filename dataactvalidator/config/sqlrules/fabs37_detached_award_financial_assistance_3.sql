-- The CFDA_Number must exist
WITH detached_award_financial_assistance_fabs37_1_{0} AS
    (SELECT submission_id,
        row_number,
        cfda_number
    FROM detached_award_financial_assistance
    WHERE submission_id = {0})
SELECT
    row_number,
    cfda_number
FROM detached_award_financial_assistance_fabs37_1_{0} AS dafa
WHERE
    dafa.row_number NOT IN (
        SELECT DISTINCT sub_dafa.row_number
        FROM detached_award_financial_assistance_fabs37_1_{0} AS sub_dafa
            JOIN cfda_program AS cfda
            ON sub_dafa.cfda_number = to_char(cfda.program_number, 'FM00.000'))