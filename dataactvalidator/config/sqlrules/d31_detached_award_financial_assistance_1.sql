-- AwardeeOrRecipientUniqueIdentifier Field must be blank for aggregate records (RecordType=1)
-- and individual recipients (BusinessTypes includes 'P').

SELECT
    row_number,
    record_type,
    business_types,
    awardee_or_recipient_uniqu,
    business_types,
    record_type
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND (record_type = 1 or LOWER(business_types) LIKE '%%p%%')
    AND COALESCE(awardee_or_recipient_uniqu, '') != ''
