-- All characters in AwardeeOrRecipientUniqueIdentifier must be numeric. If, for example, "ABCDEFGHI" is provided, it should trigger a format error.

SELECT
    row_number,
    awardee_or_recipient_uniqu
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(awardee_or_recipient_uniqu, '') != ''
    AND awardee_or_recipient_uniqu !~ '^\d\d\d\d\d\d\d\d\d$'
