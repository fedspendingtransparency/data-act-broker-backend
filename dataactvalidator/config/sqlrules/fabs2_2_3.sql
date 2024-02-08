-- The unique combination of FAIN, AwardModificationAmendmentNumber, URI, AssistanceListingNumber, and
-- AwardingSubTierAgencyCode must exist as a currently published record when the record is a deletion
-- (i.e., if CorrectionDeleteIndicator = D).
SELECT
    row_number,
    fain,
    award_modification_amendme,
    uri,
    awarding_sub_tier_agency_c,
    assistance_listing_number,
    correction_delete_indicatr,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
WHERE submission_id = {0}
    AND COALESCE(UPPER(correction_delete_indicatr), '') = 'D'
    AND NOT EXISTS (
        SELECT 1
        FROM published_fabs AS pf
        WHERE UPPER(fabs.afa_generated_unique) = UPPER(pf.afa_generated_unique)
            AND pf.is_active = TRUE
    );
