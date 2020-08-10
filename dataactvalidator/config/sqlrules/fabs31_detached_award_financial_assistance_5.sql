-- For AssistanceType of 02, 03, 04, or 05 whose ActionDate is after October 1, 2010 and ActionType = A,
-- AwardeeOrRecipientUniqueIdentifier must be active as of the ActionDate, unless the record is an aggregate
-- or PII-redacted non-aggregate record (RecordType=1 or 3) awarded to an or individual recipient (BusinessTypes
-- includes 'P'). This is an error because CorrectionDeleteIndicator is not C or the action date is after
-- January 1, 2017.
WITH detached_award_financial_assistance_fabs31_5_{0} AS
    (SELECT
        dafa_31_5.row_number,
        dafa_31_5.assistance_type,
        dafa_31_5.action_date,
        dafa_31_5.action_type,
        dafa_31_5.awardee_or_recipient_uniqu,
        dafa_31_5.business_types,
        dafa_31_5.record_type,
        dafa_31_5.correction_delete_indicatr,
        dafa_31_5.submission_id,
        dafa_31_5.afa_generated_unique
    FROM detached_award_financial_assistance AS dafa_31_5
    WHERE submission_id = {0}),
duns_short_fabs31_5_{0} AS
    (SELECT DISTINCT duns.awardee_or_recipient_uniqu,
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
    dafa.correction_delete_indicatr,
    dafa.afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance_fabs31_5_{0} AS dafa
WHERE NOT (dafa.record_type IN (1, 3)
        OR UPPER(dafa.business_types) LIKE '%%P%%'
    )
    AND COALESCE(dafa.assistance_type, '') IN ('02', '03', '04', '05')
    AND UPPER(dafa.action_type) = 'A'
    AND dafa.awardee_or_recipient_uniqu ~ '^\d\d\d\d\d\d\d\d\d$'
    AND (CASE WHEN is_date(COALESCE(dafa.action_date, '0'))
            THEN CAST(dafa.action_date AS DATE)
        END) > CAST('10/01/2010' AS DATE)
    AND (UPPER(COALESCE(dafa.correction_delete_indicatr, '')) <> 'C'
        OR (CASE WHEN is_date(COALESCE(dafa.action_date, '0'))
                THEN CAST(dafa.action_date AS DATE)
            END) >= CAST('01/01/2017' AS DATE)
    )
    AND COALESCE(dafa.awardee_or_recipient_uniqu, '') IN (SELECT DISTINCT short_duns.awardee_or_recipient_uniqu
                                                          FROM duns_short_fabs31_5_{0} AS short_duns
                                                          )
    AND dafa.row_number NOT IN (
            SELECT DISTINCT sub_dafa.row_number
            FROM detached_award_financial_assistance_fabs31_5_{0} AS sub_dafa
                JOIN duns_short_fabs31_5_{0} AS duns_short
                ON duns_short.awardee_or_recipient_uniqu = sub_dafa.awardee_or_recipient_uniqu
                AND (CASE WHEN is_date(COALESCE(sub_dafa.action_date, '0'))
                        THEN CAST(sub_dafa.action_date AS DATE)
                    END) >= CAST(duns_short.registration_date AS DATE)
                AND (CASE WHEN is_date(COALESCE(sub_dafa.action_date, '0'))
                        THEN CAST(sub_dafa.action_date AS DATE)
                    END) < CAST(duns_short.expiration_date AS DATE)
            )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
