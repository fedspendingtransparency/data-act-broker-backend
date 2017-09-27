-- For AssistanceType of 02, 03, 04, or 05 whose ActionDate is after October 1, 2010 and ActionType = A,
-- AwardeeOrRecipientUniqueIdentifier must be active as of the ActionDate, unless the record is an aggregate
-- record (RecordType=1) or individual recipient (BusinessTypes includes 'P'). This is an error because
-- CorrectionLateDeleteIndicator is not C or the action date is after January 1, 2017.

CREATE OR REPLACE function pg_temp.is_date(str text) returns boolean AS $$
BEGIN
    perform CAST(str AS DATE);
    return TRUE;
EXCEPTION WHEN others THEN
    return FALSE;
END;
$$ LANGUAGE plpgsql;

WITH detached_award_financial_assistance_fabs31_5_{0} AS
    (SELECT
        dafa_31_5.row_number,
        dafa_31_5.assistance_type,
        dafa_31_5.action_date,
        dafa_31_5.action_type,
        dafa_31_5.awardee_or_recipient_uniqu,
        dafa_31_5.business_types,
        dafa_31_5.record_type,
        dafa_31_5.correction_late_delete_ind,
        dafa_31_5.submission_id
    FROM detached_award_financial_assistance AS dafa_31_5
WHERE submission_id = {0}),

duns_short_fabs31_5_{0} AS
    (SELECT DISTINCT
        duns.awardee_or_recipient_uniqu,
        duns.expiration_date,
        duns.registration_date
    FROM duns
    JOIN detached_award_financial_assistance_fabs31_5_{0} AS sub_dafa
    ON duns.awardee_or_recipient_uniqu = sub_dafa.awardee_or_recipient_uniqu)

SELECT
    dafa.row_number,
    dafa.assistance_type,
    dafa.action_date,
    dafa.action_type,
    dafa.awardee_or_recipient_uniqu,
    dafa.business_types,
    dafa.record_type,
    dafa.correction_late_delete_ind
FROM detached_award_financial_assistance_fabs31_5_{0} AS dafa
WHERE NOT (dafa.record_type = 1 or LOWER(dafa.business_types) LIKE '%%p%%')
    AND COALESCE(dafa.assistance_type, '') IN ('02', '03', '04', '05')
    AND dafa.action_type = 'A'
    AND dafa.awardee_or_recipient_uniqu ~ '^\d\d\d\d\d\d\d\d\d$'
    AND (CASE
        WHEN pg_temp.is_date(COALESCE(dafa.action_date, '0'))
        THEN
            CAST(dafa.action_date as DATE)
        END) > CAST('10/01/2010' as DATE)
    AND (COALESCE(dafa.correction_late_delete_ind,'') != 'C'
        OR (CASE
            WHEN pg_temp.is_date(COALESCE(dafa.action_date, '0'))
            THEN
                CAST(dafa.action_date as DATE)
            END) >= CAST('01/01/2017' as DATE)
    )
    AND COALESCE(dafa.awardee_or_recipient_uniqu, '') IN (
        SELECT DISTINCT short_duns.awardee_or_recipient_uniqu
        FROM duns_short_fabs31_5_{0} AS short_duns
    )
    AND dafa.row_number NOT IN (
            SELECT DISTINCT sub_dafa.row_number
            FROM detached_award_financial_assistance_fabs31_5_{0} as sub_dafa
                JOIN duns_short_fabs31_5_{0} AS duns_short
                ON duns_short.awardee_or_recipient_uniqu = sub_dafa.awardee_or_recipient_uniqu
                AND ((CASE WHEN pg_temp.is_date(COALESCE(sub_dafa.action_date, '0'))
                    THEN CAST(sub_dafa.action_date as Date)
                    END) >= CAST(duns_short.registration_date as DATE)
                AND (CASE WHEN pg_temp.is_date(COALESCE(sub_dafa.action_date, '0'))
                    THEN CAST(sub_dafa.action_date as Date)
                    END) < CAST(duns_short.expiration_date as DATE)
                )
            )