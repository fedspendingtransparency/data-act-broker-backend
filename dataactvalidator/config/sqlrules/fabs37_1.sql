-- For new (ActionType = A) or mixed aggregate (ActionType = E) assistance awards specifically,
-- the AssistanceListingNumber must be active as of the ActionDate. This does not apply to correction records
-- (those with CorrectionDeleteIndicator = C and delete records).

WITH fabs37_1_{0} AS
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
FROM fabs37_1_{0} AS fabs
WHERE UPPER(fabs.action_type) IN ('A', 'E')
    AND UPPER(COALESCE(correction_delete_indicatr, '')) NOT IN ('C', 'D')
    AND fabs.row_number NOT IN (
        SELECT DISTINCT sub_fabs.row_number
        FROM fabs37_1_{0} AS sub_fabs
            JOIN assistance_listing AS al
                ON sub_fabs.assistance_listing_number = UPPER(al.program_number)
                AND ((al.published_date <= sub_fabs.action_date
                        AND al.archived_date = ''
                    )
                    OR (sub_fabs.action_date <= al.archived_date
                        AND al.archived_date <> ''
                    )
                )
    );
