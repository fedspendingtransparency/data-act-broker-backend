-- LegalEntityCountryCode is required for all submissions except delete records, but was not provided in this row.
SELECT
    row_number,
    legal_entity_country_code,
    correction_delete_indicatr,
    afa_generated_unique AS "uniqueid_afa_generated_unique"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(legal_entity_country_code, '') = ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
