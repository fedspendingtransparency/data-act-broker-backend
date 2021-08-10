-- When provided, AwardeeOrRecipientUEI and/or AwardeeOrRecipientDUNS must be registered (not necessarily active)
-- in SAM, unless the ActionDate is before October 1, 2010.
WITH detached_award_financial_assistance_31_4_1_{0} AS
    (SELECT unique_award_key,
        row_number,
        assistance_type,
        action_date,
        awardee_or_recipient_uniqu,
        uei,
        afa_generated_unique
    FROM detached_award_financial_assistance AS dafa
    WHERE submission_id = {0}
        AND (CASE WHEN is_date(COALESCE(action_date, '0'))
             THEN CAST(action_date AS DATE)
             END) > CAST('10/01/2010' AS DATE)
        AND (
            (COALESCE(dafa.awardee_or_recipient_uniqu, '') <> ''
             AND NOT EXISTS (
                    SELECT 1
                    FROM duns
                    WHERE dafa.awardee_or_recipient_uniqu = duns.awardee_or_recipient_uniqu
                )
            )
            OR (COALESCE(dafa.uei, '') <> ''
                AND NOT EXISTS (
                    SELECT 1
                    FROM duns
                    WHERE dafa.uei = duns.uei
                )
            )
        )
        AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'
    ),
min_dates_{0} AS
    (SELECT unique_award_key,
        MIN(cast_as_date(action_date)) AS min_date
    FROM published_award_financial_assistance AS pafa
    WHERE is_active IS TRUE
        AND EXISTS (
            SELECT 1
            FROM detached_award_financial_assistance_31_4_1_{0} AS dafa
            WHERE pafa.unique_award_key = dafa.unique_award_key
        )
    GROUP BY unique_award_key
    )
SELECT
    row_number,
    assistance_type,
    action_date,
    awardee_or_recipient_uniqu,
    uei,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance_31_4_1_{0} AS dafa
WHERE NOT EXISTS (
    SELECT 1
    FROM min_dates_{0} AS md
    WHERE dafa.unique_award_key = md.unique_award_key
        AND COALESCE(assistance_type, '') IN ('06', '07', '08', '09', '10', '11')
        AND min_date < CAST('04/04/2022' AS DATE)
);
