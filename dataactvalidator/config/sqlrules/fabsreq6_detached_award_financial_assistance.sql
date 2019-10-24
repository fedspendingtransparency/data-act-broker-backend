-- BusinessTypes is required for all submissions except delete records, but was not provided in this row.
SELECT
    row_number,
    business_types,
    correction_delete_indicatr,
    afa_generated_unique AS "uniqueid_afa_generated_unique"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(business_types, '') = ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
