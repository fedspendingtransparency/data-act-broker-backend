-- When AwardeeOrRecipientUEI is provided, it must be twelve characters.
-- When AwardeeOrRecipientDUNS is provided, it must be nine digits.
SELECT
    row_number,
    assistance_type,
    action_date,
    awardee_or_recipient_uniqu,
    uei,
    business_types,
    record_type,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND (
        (COALESCE(awardee_or_recipient_uniqu, '') <> ''
            AND awardee_or_recipient_uniqu !~ '^\d{{9}}$'
        )
        OR (COALESCE(uei, '') <> ''
            AND uei !~ '^[a-zA-Z\d]{{12}}$'
        )
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
