-- For AssistanceType of 02, 03, 04, or 05 whose ActionDate is after October 1, 2010 and ActionType = A,
-- AwardeeOrRecipientUniqueIdentifier should be active as of the ActionDate, unless the record is an aggregate
-- record (RecordType=1) or individual recipient (BusinessTypes includes 'P'). This is a warning because
-- CorrectionLateDeleteIndicator is C and the action date is before January 1, 2017.

CREATE OR REPLACE function pg_temp.is_date(str text) returns boolean AS $$
BEGIN
    perform CAST(str AS DATE);
    return TRUE;
EXCEPTION WHEN others THEN
    return FALSE;
END;
$$ LANGUAGE plpgsql;

WITH detached_award_financial_assistance_d44_4_{0} AS
    (SELECT row_number,
        assistance_type,
        action_date,
        action_type,
        awardee_or_recipient_uniqu,
        business_types,
        record_type,
        correction_late_delete_ind,
        submission_id
    FROM detached_award_financial_assistance
WHERE submission_id = {0})

SELECT
    row_number,
    assistance_type,
    action_date,
    action_type,
    awardee_or_recipient_uniqu,
    business_types,
    correction_late_delete_ind,
    record_type
FROM detached_award_financial_assistance_d44_4_{0} AS dafa
WHERE NOT (record_type = 1 or LOWER(business_types) LIKE '%%p%%')
    AND COALESCE(assistance_type, '') IN ('02', '03', '04', '05')
    AND (CASE
        WHEN pg_temp.is_date(COALESCE(dafa.action_date, '0'))
        THEN
            CAST(dafa.action_date as DATE)
    END) > CAST('10/01/2010' as DATE)
    AND awardee_or_recipient_uniqu ~ '^\d\d\d\d\d\d\d\d\d$'
    AND COALESCE(dafa.awardee_or_recipient_uniqu, '') IN (
        SELECT DISTINCT duns.awardee_or_recipient_uniqu
        FROM duns
    )
    AND dafa.action_type = 'A'
    AND dafa.row_number NOT IN (
            SELECT DISTINCT sub_dafa.row_number
            FROM detached_award_financial_assistance_d44_4_{0} as sub_dafa
                JOIN duns
                ON (sub_dafa.awardee_or_recipient_uniqu IS NOT DISTINCT FROM duns.awardee_or_recipient_uniqu
                AND (CASE WHEN pg_temp.is_date(COALESCE(sub_dafa.action_date, '0'))
                    THEN CAST(sub_dafa.action_date as Date)
                    END) >= CAST(duns.activation_date as DATE)
                AND (CASE WHEN pg_temp.is_date(COALESCE(sub_dafa.action_date, '0'))
                    THEN CAST(sub_dafa.action_date as Date)
                    END) < CAST(duns.expiration_date as DATE)
                )
            )
    AND COALESCE(dafa.correction_late_delete_ind,'') = 'C'
    AND (CASE
        WHEN pg_temp.is_date(COALESCE(dafa.action_date, '0'))
        THEN
            CAST(dafa.action_date as DATE)
    END) < CAST('01/01/2017' as DATE)