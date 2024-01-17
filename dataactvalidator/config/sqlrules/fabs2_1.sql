-- The combination of FAIN, AwardModificationAmendmentNumber, URI, AssistanceListingNumber, and
-- AwardingSubTierAgencyCode must be unique within the submission file.
SELECT
    row_number,
    fain,
    award_modification_amendme,
    uri,
    awarding_sub_tier_agency_c,
    cfda_number AS "assistance_listing_number",
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM (
    SELECT row_number,
        fain,
        award_modification_amendme,
        uri,
        awarding_sub_tier_agency_c,
        cfda_number,
        afa_generated_unique,
        ROW_NUMBER() OVER (PARTITION BY
            UPPER(afa_generated_unique)
            ORDER BY row_number
        ) AS row
    FROM fabs
    WHERE submission_id = {0}
    ) duplicates
WHERE duplicates.row > 1