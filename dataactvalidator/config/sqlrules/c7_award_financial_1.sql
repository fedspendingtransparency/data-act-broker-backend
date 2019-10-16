-- GrossOutlaysDeliveredOrdersPaidTotal (CPE) = USSGL(4902 + 4908 + 4982). This applies to the award level.
SELECT
    row_number,
    gross_outlays_delivered_or_cpe,
    ussgl490200_delivered_orde_cpe,
    ussgl490800_authority_outl_cpe,
    ussgl498200_upward_adjustm_cpe,
    COALESCE(gross_outlays_delivered_or_cpe, 0) - (COALESCE(ussgl490200_delivered_orde_cpe, 0) +
                                                   COALESCE(ussgl490800_authority_outl_cpe, 0) +
                                                   COALESCE(ussgl498200_upward_adjustm_cpe, 0)) AS "variance"
FROM award_financial
WHERE submission_id = {0}
    AND COALESCE(gross_outlays_delivered_or_cpe, 0) <>
        COALESCE(ussgl490200_delivered_orde_cpe, 0) +
        COALESCE(ussgl490800_authority_outl_cpe, 0) +
        COALESCE(ussgl498200_upward_adjustm_cpe, 0);
