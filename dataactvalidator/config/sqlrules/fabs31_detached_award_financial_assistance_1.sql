-- AwardeeOrRecipientDUNS and AwardeeOrRecipientUEI Fields must be blank for aggregate and PII-redacted non-aggregate
-- records (RecordType = 1 or 3, regardless of the BusinessTypes value) and individual recipients (BusinessTypes
-- includes 'P', regardless of the RecordType value).
SELECT
    row_number,
    record_type,
    business_types,
    awardee_or_recipient_uniqu,
    uei,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND (record_type IN (1, 3)
        OR UPPER(business_types) LIKE '%%P%%'
    )
    AND (COALESCE(awardee_or_recipient_uniqu, '') <> ''
        OR COALESCE(uei, '') <> ''
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
