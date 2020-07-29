-- For AssistanceType of 02, 03, 04, or 05 whose ActionDate is after October 1, 2010
-- and ActionType = B, C, or D, AwardeeOrRecipientUniqueIdentifier should be active
-- on the ActionDate, unless the record is an aggregate or PII-redacted non-aggregate record (RecordType=1 or 3) or
-- awarded to an individual recipient (BusinessTypes includes 'P').

WITH detached_award_financial_assistance_fabs31_7_{0} AS
    (SELECT row_number,
        assistance_type,
        action_date,
        action_type,
        awardee_or_recipient_uniqu,
        business_types,
        record_type,
        submission_id,
        correction_delete_indicatr,
        afa_generated_unique
    FROM detached_award_financial_assistance
    WHERE submission_id = {0}),
duns_fabs31_7_{0} AS
    (SELECT DISTINCT
        duns_fabs31.awardee_or_recipient_uniqu,
        duns_fabs31.registration_date,
        duns_fabs31.expiration_date
    FROM duns AS duns_fabs31
    JOIN detached_award_financial_assistance_fabs31_7_{0} AS sub_dafa
        ON duns_fabs31.awardee_or_recipient_uniqu = sub_dafa.awardee_or_recipient_uniqu)
SELECT
    dafa.row_number,
    dafa.assistance_type,
    dafa.action_date,
    dafa.action_type,
    dafa.awardee_or_recipient_uniqu,
    dafa.business_types,
    dafa.record_type,
    dafa.afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance_fabs31_7_{0} AS dafa
WHERE NOT (dafa.record_type IN (1, 3)
        OR UPPER(dafa.business_types) LIKE '%%P%%'
    )
    AND COALESCE(dafa.assistance_type, '') IN ('02', '03', '04', '05')
    AND UPPER(dafa.action_type) IN ('B', 'C', 'D')
    AND dafa.awardee_or_recipient_uniqu ~ '^\d\d\d\d\d\d\d\d\d$'
    AND (CASE WHEN is_date(COALESCE(dafa.action_date, '0'))
            THEN CAST(dafa.action_date AS DATE)
        END) > CAST('10/01/2010' AS DATE)
    AND COALESCE(dafa.awardee_or_recipient_uniqu, '') IN (SELECT DISTINCT short_duns.awardee_or_recipient_uniqu
                                                          FROM duns_fabs31_7_{0} AS short_duns
                                                          )
    AND dafa.row_number NOT IN (
            SELECT DISTINCT sub_dafa.row_number
            FROM detached_award_financial_assistance_fabs31_7_{0} AS sub_dafa
                JOIN duns_fabs31_7_{0} AS duns_short
                ON duns_short.awardee_or_recipient_uniqu = sub_dafa.awardee_or_recipient_uniqu
                AND ((CASE WHEN is_date(COALESCE(sub_dafa.action_date, '0'))
                    THEN CAST(sub_dafa.action_date AS DATE)
                    END) >= CAST(duns_short.registration_date AS DATE)
                AND (CASE WHEN is_date(COALESCE(sub_dafa.action_date, '0'))
                    THEN CAST(sub_dafa.action_date AS DATE)
                    END) < CAST(duns_short.expiration_date AS DATE)
                )
            )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
