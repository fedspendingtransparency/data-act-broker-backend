-- AssistanceListingNumber must be in XX.XXX format where # represents a number from 0 to 9.
SELECT
    row_number,
    assistance_listing_number,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND assistance_listing_number !~ '^\d\d\.\d\d\d$'
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
