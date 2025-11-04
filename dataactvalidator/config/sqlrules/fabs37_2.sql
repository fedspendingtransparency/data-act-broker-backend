-- For other assistance awards (ActionType = B, C, or D), the AssistanceListingNumber need NOT be active as of the
-- ActionDate. This does not apply to correction records (those with CorrectionDeleteIndicator = C).
-- Should not be active (action_date <= archived_date and when archived date exists)
-- If the ActionDate is < published_date, should trigger a warning.
WITH fabs37_2_{0} AS
    (SELECT submission_id,
        row_number,
        UPPER(assistance_listing_number) AS assistance_listing_number,
        action_type,
        correction_delete_indicatr,
        action_date,
        afa_generated_unique
    FROM fabs
    WHERE submission_id = {0})
SELECT
    row_number,
    assistance_listing_number,
    action_type,
    correction_delete_indicatr,
    action_date,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs37_2_{0} AS fabs
WHERE UPPER(fabs.action_type) IN ('B', 'C', 'D')
    AND UPPER(COALESCE(correction_delete_indicatr, '')) NOT IN ('C', 'D')
    AND fabs.row_number IN (
        SELECT DISTINCT sub_fabs.row_number
        FROM fabs37_2_{0} AS sub_fabs
            JOIN assistance_listing AS al
                ON sub_fabs.assistance_listing_number = UPPER(al.program_number)
                AND (sub_fabs.action_date <= al.published_date
                     OR (sub_fabs.action_date >= al.archived_date
                         AND al.archived_date <> ''
                     )
                )
    );
