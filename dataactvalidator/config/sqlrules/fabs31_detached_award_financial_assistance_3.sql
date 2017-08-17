-- If the record is not an aggregate record (RecordType=1) or individual recipient
-- (BusinessTypes includes 'P') and AwardeeOrRecipientUniqueIdentifier is provided, it must be nine digits.

SELECT
    row_number,
    assistance_type,
    action_date,
    awardee_or_recipient_uniqu,
    business_types,
    record_type
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND NOT (record_type = 1 or LOWER(business_types) LIKE '%%p%%')
    AND COALESCE(awardee_or_recipient_uniqu, '') != ''
    AND awardee_or_recipient_uniqu !~ '^\d\d\d\d\d\d\d\d\d$'