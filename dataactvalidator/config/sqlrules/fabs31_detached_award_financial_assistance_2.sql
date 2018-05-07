-- AwardeeOrRecipientUniqueIdentifier is required for AssistanceType of 02, 03, 04, or 05 where ActionDate is after
-- October 1, 2010, unless the record is an aggregate or PII-redacted non-aggregate record (RecordType = 1 or 3) or
-- individual recipient (BusinessTypes includes "P").
CREATE OR REPLACE function pg_temp.is_date(str text) returns boolean AS $$
BEGIN
    perform CAST(str AS DATE);
    return TRUE;
EXCEPTION WHEN others THEN
    return FALSE;
END;
$$ LANGUAGE plpgsql;

SELECT
    row_number,
    assistance_type,
    action_date,
    awardee_or_recipient_uniqu,
    business_types,
    record_type
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND NOT (record_type IN (1, 3)
            OR UPPER(business_types) LIKE '%%P%%'
    )
    AND COALESCE(assistance_type, '') IN ('02', '03', '04', '05')
    AND (CASE
            WHEN pg_temp.is_date(COALESCE(action_date, '0'))
            THEN CAST(action_date AS DATE)
        END) > CAST('10/01/2010' AS DATE)
    AND COALESCE(awardee_or_recipient_uniqu, '') = '';
