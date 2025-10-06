-- When provided, AwardingOfficeCode must be a valid value from the Federal Hierarchy, including being designated
-- specifically as an Assistance Awarding Office in the hierarchy at the time the award was signed (per the Action Date)
SELECT
    row_number,
    awarding_office_code,
    action_date,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(awarding_office_code, '') <> ''
    AND NOT EXISTS (
        SELECT 1
        FROM office
        WHERE UPPER(office.office_code) = UPPER(fabs.awarding_office_code)
            AND office.financial_assistance_awards_office IS TRUE
            AND office.effective_start_date <= cast_as_date(fabs.action_date)
		    AND COALESCE(office.effective_end_date, NOW() + INTERVAL '1 year') > cast_as_date(fabs.action_date)
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
