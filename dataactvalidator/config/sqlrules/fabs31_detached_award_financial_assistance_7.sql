-- When ActionDate is after October 1, 2010 and ActionType = B, C, or D, AwardeeOrRecipientUEI and
-- AwardeeOrRecipientDUNS should (when provided) have an active registration in SAM as of the ActionDate, except where
-- FederalActionObligation is <=0 and ActionType = D.
WITH detached_award_financial_assistance_fabs31_7_{0} AS
    (SELECT row_number,
        action_date,
        action_type,
        awardee_or_recipient_uniqu,
        uei,
        business_types,
        record_type,
        federal_action_obligation,
        afa_generated_unique
    FROM detached_award_financial_assistance AS dafa
    WHERE submission_id = {0}
        AND (COALESCE(dafa.awardee_or_recipient_uniqu, '') <> ''
            OR COALESCE(dafa.uei, '') <> ''
        )
        AND UPPER(dafa.action_type) IN ('B', 'C', 'D')
        AND (CASE WHEN is_date(COALESCE(dafa.action_date, '0'))
             THEN CAST(dafa.action_date AS DATE)
             END) > CAST('10/01/2010' AS DATE)
        AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D')
SELECT
    dafa.row_number,
    dafa.action_date,
    dafa.action_type,
    dafa.awardee_or_recipient_uniqu,
    dafa.uei,
    dafa.federal_action_obligation,
    dafa.afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance_fabs31_7_{0} AS dafa
WHERE NOT (dafa.federal_action_obligation <= 0
        AND UPPER(dafa.action_type) = 'D')
    AND COALESCE(dafa.awardee_or_recipient_uniqu, '') = ''
    AND NOT EXISTS (
        SELECT 1
        FROM duns
        WHERE (
            dafa.uei = duns.uei
            AND (CASE WHEN is_date(COALESCE(dafa.action_date, '0'))
                  THEN CAST(dafa.action_date AS DATE)
                  END) >= CAST(duns.registration_date AS DATE)
            AND (CASE WHEN is_date(COALESCE(dafa.action_date, '0'))
                 THEN CAST(dafa.action_date AS DATE)
                 END) < CAST(duns.expiration_date AS DATE)
        )
   )
UNION
SELECT
    dafa.row_number,
    dafa.action_date,
    dafa.action_type,
    dafa.awardee_or_recipient_uniqu,
    dafa.uei,
    dafa.federal_action_obligation,
    dafa.afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance_fabs31_7_{0} AS dafa
WHERE NOT (dafa.federal_action_obligation <= 0
        AND UPPER(dafa.action_type) = 'D')
    AND COALESCE(dafa.awardee_or_recipient_uniqu, '') <> ''
    AND NOT EXISTS (
        SELECT 1
        FROM duns
        WHERE (
            dafa.awardee_or_recipient_uniqu = duns.awardee_or_recipient_uniqu
            AND (CASE WHEN is_date(COALESCE(dafa.action_date, '0'))
                  THEN CAST(dafa.action_date AS DATE)
                  END) >= CAST(duns.registration_date AS DATE)
            AND (CASE WHEN is_date(COALESCE(dafa.action_date, '0'))
                 THEN CAST(dafa.action_date AS DATE)
                 END) < CAST(duns.expiration_date AS DATE)
        )
   );
