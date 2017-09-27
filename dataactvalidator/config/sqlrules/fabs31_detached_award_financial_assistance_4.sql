-- For AssistanceType of 02, 03, 04, or 05 whose ActionDate is after October 1, 2010,
-- AwardeeOrRecipientUniqueIdentifier must be found in our records, unless the record
-- is an aggregate record (RecordType=1) or individual recipient (BusinessTypes includes 'P').

CREATE OR REPLACE function pg_temp.is_date(str text) returns boolean AS $$
BEGIN
    perform CAST(str AS DATE);
    return TRUE;
EXCEPTION WHEN others THEN
    return FALSE;
END;
$$ LANGUAGE plpgsql;

WITH detached_award_financial_assistance_fabs31_4_{0} AS
    (SELECT
        submission_id,
        row_number,
        assistance_type,
        action_date,
        awardee_or_recipient_uniqu,
        business_types,
        record_type
    FROM detached_award_financial_assistance
    WHERE submission_id = {0})

SELECT
    row_number,
    assistance_type,
    action_date,
    awardee_or_recipient_uniqu,
    business_types,
    record_type
FROM detached_award_financial_assistance_fabs31_4_{0} AS dafa
WHERE NOT (record_type = 1 or LOWER(business_types) LIKE '%%p%%')
    AND awardee_or_recipient_uniqu ~ '^\d\d\d\d\d\d\d\d\d$'
    AND COALESCE(assistance_type, '') IN ('02', '03', '04', '05')
    AND (CASE
        WHEN pg_temp.is_date(COALESCE(action_date, '0'))
        THEN
            CAST(action_date as DATE)
    END) > CAST('10/01/2010' as DATE)
    AND NOT EXISTS (
        SELECT *
        FROM duns
        WHERE dafa.awardee_or_recipient_uniqu = duns.awardee_or_recipient_uniqu
    )