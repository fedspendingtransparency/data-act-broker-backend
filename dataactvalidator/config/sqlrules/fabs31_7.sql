-- When ActionDate is after October 1, 2010 and ActionType = B, C, or D, AwardeeOrRecipientUEI should (when provided)
-- have an active registration in SAM as of the ActionDate, except where FederalActionObligation is <= 0 and
-- ActionType = D.
WITH fabs31_7_{0} AS
    (SELECT row_number,
        action_date,
        action_type,
        uei,
        business_types,
        record_type,
        federal_action_obligation,
        afa_generated_unique
    FROM fabs
    WHERE submission_id = {0}
        AND COALESCE(uei, '') <> ''
        AND UPPER(action_type) IN ('B', 'C', 'D')
        AND (CASE WHEN is_date(COALESCE(action_date, '0'))
             THEN CAST(action_date AS DATE)
             END) > CAST('10/01/2010' AS DATE)
        AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D')
SELECT
    row_number,
    action_date,
    action_type,
    uei,
    federal_action_obligation,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs31_7_{0} AS fabs
WHERE NOT (federal_action_obligation <= 0
        AND UPPER(action_type) = 'D')
    AND NOT EXISTS (
        SELECT 1
        FROM sam_recipient
        WHERE (
            UPPER(fabs.uei) = UPPER(sam_recipient.uei)
            AND (CASE WHEN is_date(COALESCE(fabs.action_date, '0'))
                  THEN CAST(fabs.action_date AS DATE)
                  END) >= CAST(sam_recipient.registration_date AS DATE)
            AND (CASE WHEN is_date(COALESCE(fabs.action_date, '0'))
                 THEN CAST(fabs.action_date AS DATE)
                 END) < CAST(sam_recipient.expiration_date AS DATE)
        )
   );
