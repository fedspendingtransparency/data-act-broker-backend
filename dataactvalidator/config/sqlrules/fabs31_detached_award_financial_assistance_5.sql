-- When ActionDate is after October 1, 2010 and ActionType = A, AwardeeOrRecipientUEI should (when provided) have an
-- active registration in SAM as of the ActionDate.
WITH detached_award_financial_assistance_fabs31_5_{0} AS
    (SELECT
        row_number,
        action_date,
        action_type,
        uei,
        afa_generated_unique
    FROM detached_award_financial_assistance AS dafa
    WHERE submission_id = {0}
        AND COALESCE(uei, '') <> ''
        AND UPPER(action_type) = 'A'
        AND (CASE WHEN is_date(COALESCE(action_date, '0'))
             THEN CAST(action_date AS DATE)
             END) > CAST('10/01/2010' AS DATE)
        AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D')
SELECT
    dafa.row_number,
    dafa.action_date,
    dafa.action_type,
    dafa.uei,
    dafa.afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance_fabs31_5_{0} AS dafa
WHERE NOT EXISTS (
        SELECT 1
        FROM sam_recipient
        WHERE (
            UPPER(dafa.uei) = UPPER(sam_recipient.uei)
            AND (CASE WHEN is_date(COALESCE(dafa.action_date, '0'))
                 THEN CAST(dafa.action_date AS DATE)
                 END) >= CAST(sam_recipient.registration_date AS DATE)
            AND (CASE WHEN is_date(COALESCE(dafa.action_date, '0'))
                 THEN CAST(dafa.action_date AS DATE)
                 END) < CAST(sam_recipient.expiration_date AS DATE)
        )
    );
