-- ActionType should be "A" for the initial transaction of a new, non-aggregate award (RecordType = 2 or 3) and
-- “A” or “E” for a new aggregate award (RecordType = 1). An aggregate record transaction is considered the initial
-- transaction of a new award if it contains a unique combination of URI + AwardingSubTierAgencyCode when compared to
-- currently published FABS records of the same RecordType. A non-aggregate (RecordType = 2 or 3) transaction is
-- considered the initial transaction of a new award if it contains a unique combination of FAIN +
-- AwardingSubTierAgencyCode when compared to currently published non-aggregate FABS records (RecordType = 2 or 3) of
-- the same RecordType.
SELECT
    dafa.row_number,
    dafa.fain,
    dafa.uri,
    dafa.awarding_sub_tier_agency_c,
    dafa.action_type,
    dafa.record_type,
    dafa.correction_delete_indicatr,
    dafa.afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance AS dafa
WHERE dafa.submission_id = {0}
    AND COALESCE(UPPER(dafa.correction_delete_indicatr), '') NOT IN ('C', 'D')
    AND NOT EXISTS (
        SELECT 1
        FROM published_fabs AS pf
        WHERE dafa.unique_award_key = pf.unique_award_key
            AND pf.is_active IS TRUE
    )
    AND NOT ((COALESCE(UPPER(dafa.action_type), '') = 'E'
            AND dafa.record_type = 1)
            OR COALESCE(UPPER(dafa.action_type), '') = 'A'
        );

