-- AwardeeOrRecipientUEI is required where ActionDate is after October 1, 2010, unless the record is an aggregate or
-- PII-redacted non-aggregate record (RecordType = 1 or 3) or the recipient is an individual
-- (BusinessTypes includes 'P'). For AssistanceType 06, 07, 08, 09, 10, or 11, if the base award (the earliest record
-- with the same unique award key) has an ActionDate prior to October 1, 2022, this will produce a warning rather than a
-- fatal error.
WITH fabs31_2_2_{0} AS
    (SELECT unique_award_key,
    	row_number,
    	assistance_type,
    	action_date,
    	uei,
    	business_types,
    	record_type,
    	afa_generated_unique
    FROM fabs
    WHERE submission_id = {0}
        AND NOT (record_type IN (1, 3)
            OR UPPER(business_types) LIKE '%%P%%'
        )
        AND (CASE WHEN is_date(COALESCE(action_date, '0'))
             THEN CAST(action_date AS DATE)
             END) > CAST('10/01/2010' AS DATE)
        AND COALESCE(uei, '') = ''
        AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'),
min_dates_{0} AS
    (SELECT unique_award_key,
        MIN(cast_as_date(action_date)) AS min_date
    FROM published_fabs AS pf
    WHERE is_active IS TRUE
        AND EXISTS (
            SELECT 1
            FROM fabs31_2_2_{0} AS fabs
            WHERE pf.unique_award_key = fabs.unique_award_key)
    GROUP BY unique_award_key)
SELECT
    row_number,
    assistance_type,
    action_date,
    uei,
    business_types,
    record_type,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs31_2_2_{0} AS fabs
LEFT JOIN min_dates_{0} AS md
    ON fabs.unique_award_key = md.unique_award_key
WHERE (
    COALESCE(assistance_type, '') IN ('06', '07', '08', '09', '10', '11')
    AND CASE WHEN md.min_date IS NOT NULL
         THEN min_date < CAST('10/01/2022' AS DATE)
         ELSE (CASE WHEN is_date(COALESCE(action_date, '0'))
               THEN CAST(action_date AS DATE)
               END) < CAST('10/01/2022' AS DATE)
    END
);
