-- The DUNS must be blank for aggregate records (i.e., when RecordType = 1) and individual recipients
-- (i.e., when BusinessTypes includes "P").


SELECT
    row_number,
    awardee_or_recipient_uniqu,
    record_type,
    business_types
FROM detached_award_financial_assistance as dafa
WHERE submission_id = {0}
    AND record_type = 1
    AND POSITION('P' IN UPPER(business_types)) > 0
    AND COALESCE(awardee_or_recipient_uniqu, '') = ''