-- When ActionDate is after October 1, 2010 and ActionType = A, AwardeeOrRecipientUEI should (when provided)
-- have an active registration in SAM as of the ActionDate, unless, when ActionDate is after October 1, 2024,
-- LegalEntityCountryCode is a foreign country.
WITH fabs31_5_{0} AS
    (SELECT
        row_number,
        action_date,
        action_type,
        uei,
        legal_entity_country_code,
        afa_generated_unique,
        correction_delete_indicatr
    FROM fabs
    WHERE submission_id = {0}
        AND COALESCE(uei, '') <> ''
        AND UPPER(action_type) = 'A'
        AND (CASE WHEN is_date(COALESCE(action_date, '0'))
             THEN CAST(action_date AS DATE)
             END) > CAST('10/01/2010' AS DATE)
        AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D'),
active_recipient_{0} AS
    (SELECT *
    FROM fabs31_5_{0} AS fabs
    WHERE NOT EXISTS (
        SELECT 1
        FROM sam_recipient
        WHERE (
            UPPER(fabs.uei) = UPPER(sam_recipient.uei)
            AND (CASE WHEN is_date(COALESCE(fabs.action_date, '0'))
                 THEN CAST(fabs.action_date AS DATE)
                 END) >= CAST(sam_recipient.registration_date AS DATE)
            AND (CASE WHEN is_date(COALESCE(fabs.action_date, '0'))
                 THEN CAST(fabs.action_date AS DATE)
                 END) < CAST(sam_recipient.expiration_date AS DATE)))),
special_exception_{0} AS
    (SELECT *
    FROM active_recipient_{0} AS fabs
    WHERE NOT (
         UPPER(legal_entity_country_code) <> 'USA'
         AND (CASE WHEN is_date(COALESCE(action_date, '0'))
              THEN CAST(action_date AS DATE)
              END) > CAST('10/01/2024' AS DATE)
         AND EXISTS (
            SELECT 1
            FROM sam_recipient_unregistered AS sru
            WHERE UPPER(fabs.uei) = UPPER(sru.uei))))
SELECT
    row_number,
    action_date,
    action_type,
    legal_entity_country_code,
    uei,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM special_exception_{0} AS fabs
WHERE NOT (
          (CASE WHEN is_date(COALESCE(action_date, '0'))
           THEN CAST(action_date AS DATE)
           END) < CAST('01/01/2017' AS DATE)
           AND UPPER(COALESCE(correction_delete_indicatr, '')) = 'C');
