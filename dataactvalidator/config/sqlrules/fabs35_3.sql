-- LegalEntityZIP5 + LegalEntityZIPLast4 is not a valid 9 digit zip.
SELECT
    row_number,
    legal_entity_zip5,
    legal_entity_zip_last4,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(legal_entity_zip5, '') <> ''
    AND COALESCE(legal_entity_zip_last4, '') <> ''
    AND NOT EXISTS (
        SELECT 1
        FROM zips AS z
        WHERE fabs.legal_entity_zip5 = z.zip5
            AND fabs.legal_entity_zip_last4 = z.zip_last4
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
