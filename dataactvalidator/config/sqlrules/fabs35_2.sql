-- LegalEntityZIP5 is not a valid zip code.
SELECT
    row_number,
    legal_entity_zip5,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(legal_entity_zip5, '') <> ''
    AND NOT EXISTS (
        SELECT 1
        FROM zips AS z
        WHERE z.zip5 = fabs.legal_entity_zip5
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
