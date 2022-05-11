-- LegalEntityForeignCityName is required for foreign recipients (i.e., when LegalEntityCountryCode <> USA)
-- for non-aggregate and PII-redacted non-aggregate records (RecordType = 2 or 3).
SELECT
    row_number,
    legal_entity_country_code,
    record_type,
    legal_entity_foreign_city,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND UPPER(legal_entity_country_code) <> 'USA'
    AND record_type IN (2, 3)
    AND (legal_entity_foreign_city IS NULL
        OR legal_entity_foreign_city = ''
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
