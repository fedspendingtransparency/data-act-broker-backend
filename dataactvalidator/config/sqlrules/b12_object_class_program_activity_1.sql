SELECT op.row_number, op.bydirectreimbursablefundingsource
FROM object_class_program_activity as op
WHERE submission_id = {}
    AND (ussgl480100_undeliveredordersobligationsunpaid_fyb IS NOT NULL
        OR ussgl480100_undeliveredordersobligationsunpaid_cpe IS NOT NULL
        OR ussgl483100_undeliveredordersobligtransferredunpaid_cpe IS NOT NULL
        OR ussgl488100_upadjsprioryearundelivordersobligunpaid_cpe IS NOT NULL
        OR ussgl490100_deliveredordersobligationsunpaid_fyb IS NOT NULL
        OR ussgl490100_deliveredordersobligationsunpaid_cpe IS NOT NULL
        OR ussgl493100_deliveredordersobligstransferredunpaid_cpe IS NOT NULL
        OR ussgl498100_upadjsprioryeardeliveredordersobligunpaid_cpe IS NOT NULL
        OR ussgl480200_undeliveredordersobligationsprepaidadv_cpe IS NOT NULL
        OR ussgl480200_undeliveredordersobligationsprepaidadv_fyb IS NOT NULL
        OR ussgl483200_undeliveredordersobligtransferredppdadv_cpe IS NOT NULL
        OR ussgl488200_upadjsprioryrundelivordersobligprepaidadv_cpe IS NOT NULL
        OR ussgl490200_deliveredordersobligationspaid_cpe IS NOT NULL
        OR ussgl490800_authorityoutlayednotyetdisbursed_fyb IS NOT NULL
        OR ussgl490800_authorityoutlayednotyetdisbursed_cpe IS NOT NULL
        OR ussgl498200_upadjsprioryrdelivordersobligpaid_cpe IS NOT NULL)
    AND COALESCE(bydirectreimbursablefundingsource, '') = ''