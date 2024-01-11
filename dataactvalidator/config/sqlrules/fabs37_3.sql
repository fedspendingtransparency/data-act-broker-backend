-- AssistanceListingNumber must have been registered with CFDA.gov or registered as an Assistance Listing
-- on SAM.gov (post-May 2018) at some point in time.
WITH fabs37_1_{0} AS
    (SELECT submission_id,
        row_number,
        cfda_number,
        correction_delete_indicatr,
        afa_generated_unique
    FROM fabs
    WHERE submission_id = {0})
SELECT
    row_number,
    cfda_number AS "assistance_listing_number",
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs37_1_{0} AS fabs
WHERE fabs.row_number NOT IN (
        SELECT DISTINCT sub_fabs.row_number
        FROM fabs37_1_{0} AS sub_fabs
            JOIN cfda_program AS cfda
                ON sub_fabs.cfda_number = to_char(cfda.program_number, 'FM00.000')
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
