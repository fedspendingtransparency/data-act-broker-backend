-- AssistanceListingNumber must have been registered with CFDA.gov or registered as an Assistance Listing
-- on SAM.gov (post-May 2018) at some point in time.
WITH fabs37_3_{0} AS
    (SELECT submission_id,
        row_number,
        UPPER(assistance_listing_number) AS assistance_listing_number,
        correction_delete_indicatr,
        afa_generated_unique
    FROM fabs
    WHERE submission_id = {0})
SELECT
    row_number,
    assistance_listing_number AS "assistance_listing_number",
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs37_3_{0} AS fabs
WHERE fabs.row_number NOT IN (
        SELECT DISTINCT sub_fabs.row_number
        FROM fabs37_3_{0} AS sub_fabs
            JOIN assistance_listing AS al
                ON sub_fabs.assistance_listing_number = UPPER(al.program_number)
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
