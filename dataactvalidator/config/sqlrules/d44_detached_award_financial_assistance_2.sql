-- When provided, the DUNS must be nine digits.

SELECT
    row_number,
    assistance_type,
    action_date,
    awardee_or_recipient_uniqu
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND COALESCE(awardee_or_recipient_uniqu, '') != ''
    AND awardee_or_recipient_uniqu !~ '^\d\d\d\d\d\d\d\d\d$'