-- When AwardeeOrRecipientUEI is provided, it must be twelve characters.
SELECT
    row_number,
    uei,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND COALESCE(uei, '') <> ''
    AND uei !~ '^[a-zA-Z\d]{{12}}$'
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
