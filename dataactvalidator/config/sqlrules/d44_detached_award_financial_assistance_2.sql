-- AwardeeOrRecipientUniqueIdentifier is required for AssistanceType of 02, 03, 04, or 05 whose
-- ActionDate after October 1, 2010.
-- When ActionType = A, must be active in the System for Award Management (SAM) on the ActionDate of the award.
-- When ActionType = B, C, or D, DUNS should exist in SAM but need not be active on the ActionDate (i.e., it may be expired).

CREATE OR REPLACE function pg_temp.is_date(str text) returns boolean AS $$
BEGIN
    perform CAST(str AS DATE);
    return TRUE;
EXCEPTION WHEN others THEN
    return FALSE;
END;
$$ LANGUAGE plpgsql;

WITH detached_award_financial_assistance_d44_2_{0} AS
    (SELECT submission_id,
        row_number,
        assistance_type,
        action_date,
        action_type,
        awardee_or_recipient_uniqu
    FROM detached_award_financial_assistance
WHERE submission_id = {0})

SELECT
    row_number,
    assistance_type,
    action_date,
    action_type,
    awardee_or_recipient_uniqu
FROM detached_award_financial_assistance_d44_2_{0} AS dafa
WHERE dafa.action_type = 'A'
    AND COALESCE(dafa.assistance_type, '') IN ('02', '03', '04', '05')
    AND (CASE
        WHEN pg_temp.is_date(COALESCE(dafa.action_date, '0'))
        THEN
            CAST(dafa.action_date as DATE)
    END) > CAST('10/01/2010' as DATE)
    AND dafa.row_number NOT IN (
            SELECT DISTINCT sub_dafa.row_number
            FROM detached_award_financial_assistance_d44_2_{0} as sub_dafa
                JOIN executive_compensation AS exec_comp
                ON (sub_dafa.awardee_or_recipient_uniqu IS NOT DISTINCT FROM exec_comp.awardee_or_recipient_uniqu
                AND (CASE WHEN pg_temp.is_date(COALESCE(sub_dafa.action_date, '0'))
                    THEN CAST(sub_dafa.action_date as Date)
                    END) >= CAST(exec_comp.activation_date as DATE)
                )
            )