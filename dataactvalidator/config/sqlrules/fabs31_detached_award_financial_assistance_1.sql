-- AwardeeOrRecipientUniqueIdentifier Field must be blank for aggregate and PII-redacted non-aggregate records
-- (RecordType=1 or 3) and individual recipients (BusinessTypes includes 'P').
SELECT
    row_number,
    record_type,
    business_types,
    awardee_or_recipient_uniqu,
    business_types,
    record_type
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND (record_type IN (1, 3)
        OR UPPER(business_types) LIKE '%%P%%'
    )
    AND COALESCE(awardee_or_recipient_uniqu, '') <> ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
