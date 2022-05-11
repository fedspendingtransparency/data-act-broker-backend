-- BusinessTypes must be one to three letters in length. BusinessTypes values must be non-repeated letters from A to X.
SELECT
    row_number,
    business_types,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND (business_types ~* '([A-X]).*\1'
        OR (business_types !~* '^[A-X]$'
            AND business_types !~* '^[A-X][A-X]$'
            AND business_types !~* '^[A-X][A-X][A-X]$'
        )
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
