-- When both the AwardeeOrRecipientDUNS and AwardeeOrRecipientUEI are provided, they must match the combination shown in
-- SAM for the same awardee or recipient. In this instance, they do not.
SELECT
    dafa.row_number,
    dafa.awardee_or_recipient_uniqu,
    dafa.uei,
    dafa.afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND COALESCE(dafa.awardee_or_recipient_uniqu, '') <> ''
    AND COALESCE(dafa.uei, '') <> ''
    AND NOT EXISTS (
        SELECT 1
        FROM duns
        WHERE dafa.awardee_or_recipient_uniqu = duns.awardee_or_recipient_uniqu
            AND dafa.uei = duns.uei
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
