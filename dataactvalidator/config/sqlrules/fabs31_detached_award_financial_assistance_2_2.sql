-- AwardeeOrRecipientDUNS or AwardeeOrRecipientUEI is required where ActionDate is after October 1, 2010, unless the
-- record is an aggregate or PII-redacted non-aggregate record (RecordType = 1 or 3) or the recipient is an individual
-- (BusinessTypes includes 'P'). For AssistanceType 06, 07, 08, 09, 10, or 11, if the base award (the earliest record
-- with the same unique award key) has an ActionDate prior to April 4, 2022, this will produce a warning rather than a
-- fatal error.
WITH detached_award_financial_assistance_31_2_2_{0} AS
    (SELECT unique_award_key,
    	row_number,
    	assistance_type,
    	action_date,
    	awardee_or_recipient_uniqu,
    	uei,
    	business_types,
    	record_type,
    	afa_generated_unique
    FROM detached_award_financial_assistance
    WHERE submission_id = {0}
        AND NOT (record_type IN (1, 3)
            OR UPPER(business_types) LIKE '%%P%%'
        )
        AND (CASE WHEN is_date(COALESCE(action_date, '0'))
             THEN CAST(action_date AS DATE)
             END) > CAST('10/01/2010' AS DATE)
        AND (COALESCE(awardee_or_recipient_uniqu, '') = ''
            AND COALESCE(uei, '') = ''
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
            FROM detached_award_financial_assistance_31_2_2_{0} AS dafa
            WHERE pafa.unique_award_key = dafa.unique_award_key)
    GROUP BY unique_award_key
    )
SELECT
    row_number,
    assistance_type,
    action_date,
    awardee_or_recipient_uniqu,
    uei,
    business_types,
    record_type,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance_31_2_2_{0} AS dafa
WHERE EXISTS (
    SELECT 1
    FROM min_dates_{0} AS md
    WHERE dafa.unique_award_key = md.unique_award_key
        AND COALESCE(assistance_type, '') IN ('06', '07', '08', '09', '10', '11')
        AND min_date < CAST('04/04/2022' AS DATE)
)
