-- If LegalEntityCongressionalDistrict is provided with an ActionDate before 20230103, then the
-- LegalEntityCongressionalDistrict should be associated with the provided LegalEntityZIP5 and LegalEntityZIPLast4
-- according to the historic USPS source data.

SELECT
    row_number,
    legal_entity_congressional,
    legal_entity_zip5,
    legal_entity_zip_last4,
    action_date,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(legal_entity_congressional, '') NOT IN ('', '90')
    AND COALESCE(legal_entity_zip5, '') <> ''
    AND cast_as_date(action_date) < '01/03/2023'
    AND CASE WHEN COALESCE(legal_entity_zip_last4, '') = ''
    THEN NOT EXISTS (
        SELECT 1
        FROM zips_historical AS zips
        WHERE fabs.legal_entity_congressional = zips.congressional_district_no
            AND fabs.legal_entity_zip5 = zips.zip5
        )
    ELSE NOT EXISTS (
        SELECT 1
        FROM zips_historical AS zips
        WHERE fabs.legal_entity_congressional = zips.congressional_district_no
            AND fabs.legal_entity_zip5 = zips.zip5
            AND fabs.legal_entity_zip_last4 = zips.zip_last4
        )
    END
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';