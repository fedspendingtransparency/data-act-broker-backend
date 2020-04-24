-- ActionType should be B, C, or D for transactions that modify existing awards. For aggregate (RecordType = 1)
-- record transactions, we consider a record a modification if its combination of URI + AwardingSubTierAgencyCode
-- matches an existing published FABS record of the same RecordType. For non-aggregate (RecordType = 2 or 3) record
-- transactions, we consider a record a modification if its combination of FAIN + AwardingSubTierCode matches those of
-- an existing published non-aggregate FABS record (RecordType = 2 or 3). This validation rule does not apply to delete
-- records (CorrectionDeleteIndicator = D.)
SELECT
    dafa.row_number,
    dafa.fain,
    dafa.uri,
    dafa.awarding_sub_tier_agency_c,
    dafa.action_type,
    dafa.correction_delete_indicatr,
    dafa.afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance AS dafa
WHERE dafa.submission_id = {0}
    AND COALESCE(UPPER(dafa.correction_delete_indicatr), '') <> 'D'
    AND COALESCE(UPPER(dafa.action_type), '') NOT IN ('B', 'C', 'D')
    AND EXISTS (
        SELECT 1
        FROM published_award_financial_assistance AS pafa
        WHERE dafa.unique_award_key = pafa.unique_award_key
            AND pafa.is_active IS TRUE
    );
