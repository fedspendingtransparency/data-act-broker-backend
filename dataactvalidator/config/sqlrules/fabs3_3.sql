-- ActionType should be B, C, or D for transactions that modify existing awards.
-- For aggregate (RecordType = 1) record transactions, we consider a record a modification if its combination of
-- URI + AwardingSubTierAgencyCode matches an existing published FABS record of the same RecordType.
-- For non-aggregate (RecordType = 2 or 3) transactions, we consider a record a modification if its combination of
-- FAIN + AwardingSubTierCode matches those of an existing published non-aggregate FABS record (RecordType = 2 or 3)
-- of the same RecordType.
SELECT
    row_number,
    fain,
    uri,
    awarding_sub_tier_agency_c,
    action_type,
    correction_delete_indicatr,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(UPPER(correction_delete_indicatr), '') NOT IN ('C', 'D')
    AND COALESCE(UPPER(action_type), '') NOT IN ('B', 'C', 'D')
    AND EXISTS (
        SELECT 1
        FROM published_fabs AS pf
        WHERE fabs.unique_award_key = pf.unique_award_key
            AND pf.is_active IS TRUE
    );
