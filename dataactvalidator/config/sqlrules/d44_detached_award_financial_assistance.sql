-- Must be a 9-digit DUNS number

SELECT
    row_number,
    awardee_or_recipient_uniqu
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND dafa.awardee_or_recipient_uniqu NOT IN (
        SELECT awardee_or_recipient_uniqu
        FROM executive_compensation as exec_comp
    )