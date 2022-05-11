-- LegalEntityCountryCode must contain a valid three character GENC country code. U.S. Territories and Freely
-- Associated States must be submitted with country code = USA and their state/territory code; they cannot be submitted
-- with their GENC country code.
SELECT
    row_number,
    legal_entity_country_code,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id={0}
    AND NOT EXISTS (
        SELECT 1
        FROM country_code AS cc
        WHERE UPPER(fabs.legal_entity_country_code) = UPPER(cc.country_code)
            AND cc.territory_free_state IS FALSE
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
