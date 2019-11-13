-- Each unique PIID (or combination of PIID/ParentAwardId) from file D1 (award procurement) should exist in
-- file C (award financial) during the same reporting period, except D1 records where FederalActionObligation = 0.
WITH award_procurement_c12_{0} AS
    (SELECT row_number,
        piid,
        parent_award_id,
        federal_action_obligation
    FROM award_procurement
    WHERE submission_id = {0}),
award_financial_c12_{0} AS
    (SELECT piid,
        parent_award_id
    FROM award_financial
    WHERE submission_id = {0})
SELECT
    ap.row_number AS "source_row_number",
    ap.piid AS "source_value_piid",
    ap.parent_award_id AS "source_value_parent_award_id",
    ap.piid AS "uniqueid_PIID",
    ap.parent_award_id AS "uniqueid_ParentAwardId"
FROM award_procurement_c12_{0} AS ap
WHERE ap.piid IS NOT NULL
    AND COALESCE(ap.federal_action_obligation, 0) <> 0
    AND NOT EXISTS (
        SELECT 1
        FROM award_financial_c12_{0} AS af
        WHERE UPPER(af.piid) = UPPER(ap.piid)
            AND UPPER(af.parent_award_id) IS NOT DISTINCT FROM UPPER(ap.parent_award_id)
    );
