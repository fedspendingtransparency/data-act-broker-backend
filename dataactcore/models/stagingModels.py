from sqlalchemy import Column, Integer, Text, Numeric, Index, Boolean
from dataactcore.models.base import Base


def concatTas(context):
    """Create a concatenated TAS string for insert into database."""
    tas1 = context.current_parameters['allocationtransferagencyidentifier']
    tas1 = tas1 if tas1 else '000'
    tas2 = context.current_parameters['agencyidentifier']
    tas2 = tas2 if tas2 else '000'
    tas3 = context.current_parameters['beginningperiodofavailability']
    tas3 = tas3 if tas3 else '0000'
    tas4 = context.current_parameters['endingperiodofavailability']
    tas4 = tas4 if tas4 else '0000'
    tas5 = context.current_parameters['availabilitytypecode']
    tas5 = tas5 if tas5 else ' '
    tas6 = context.current_parameters['mainaccountcode']
    tas6 = tas6 if tas6 else '0000'
    tas7 = context.current_parameters['subaccountcode']
    tas7 = tas7 if tas7 else '000'
    tas = '{}{}{}{}{}{}{}'.format(tas1, tas2, tas3, tas4, tas5, tas6, tas7)
    return tas


class Appropriation(Base):
    """Model for the appropriation table."""
    __tablename__ = "appropriation"

    appropriation_id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, nullable=False, index=True)
    job_id = Column(Integer, nullable=False, index=True)
    row_number = Column(Integer, nullable=False)
    adjustmentstounobligatedbalancebroughtforward_cpe = Column(Numeric)
    agencyidentifier = Column(Text)
    allocationtransferagencyidentifier = Column(Text)
    availabilitytypecode = Column(Text)
    beginningperiodofavailability = Column(Text)
    borrowingauthorityamounttotal_cpe = Column(Numeric)
    budgetauthorityappropriatedamount_cpe = Column(Numeric)
    budgetauthorityavailableamounttotal_cpe = Column(Numeric)
    budgetauthorityunobligatedbalancebroughtforward_fyb = Column(Numeric)
    contractauthorityamounttotal_cpe = Column(Numeric)
    deobligationsrecoveriesrefundsbytas_cpe = Column(Numeric)
    endingperiodofavailability = Column(Text)
    grossoutlayamountbytas_cpe = Column(Numeric)
    mainaccountcode = Column(Text)
    obligationsincurredtotalbytas_cpe = Column(Numeric)
    otherbudgetaryresourcesamount_cpe = Column(Numeric)
    spendingauthorityfromoffsettingcollectionsamounttotal_cpe = Column(Numeric)
    statusofbudgetaryresourcestotal_cpe = Column(Numeric)
    subaccountcode = Column(Text)
    unobligatedbalance_cpe = Column(Numeric)
    tas = Column(Text, index=True, nullable=False, default=concatTas, onupdate=concatTas)
    valid_record = Column(Boolean, nullable = False, default = True, server_default = "True")

    def __init__(self, **kwargs):
        # broker is set up to ignore extra columns in submitted data
        # so get rid of any extraneous kwargs before instantiating
        cleanKwargs = {k: v for k, v in kwargs.items() if hasattr(self, k)}
        super(Appropriation, self).__init__(**cleanKwargs)

class ObjectClassProgramActivity(Base):
    """Model for the object_class_program_activity table."""
    __tablename__ = "object_class_program_activity"

    object_class_program_activity_id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, nullable=False, index=True)
    job_id = Column(Integer, nullable=False, index=True)
    row_number = Column(Integer, nullable=False)
    agencyidentifier = Column(Text)
    allocationtransferagencyidentifier = Column(Text)
    availabilitytypecode = Column(Text)
    beginningperiodofavailability = Column(Text)
    bydirectreimbursablefundingsource = Column(Text)
    deobligationsrecoveriesrefundsdofprioryearbyprogramobjectclass_cpe = Column(
        "deobligationsrecoveriesrefundsprioryrbyprogobjectclass_cpe", Numeric)
    endingperiodofavailability = Column(Text)
    grossoutlayamountbyprogramobjectclass_cpe = Column(Numeric)
    grossoutlayamountbyprogramobjectclass_fyb = Column(Numeric)
    grossoutlaysdeliveredorderspaidtotal_cpe = Column(Numeric)
    grossoutlaysdeliveredorderspaidtotal_fyb = Column(Numeric)
    grossoutlaysundeliveredordersprepaidtotal_cpe = Column(Numeric)
    grossoutlaysundeliveredordersprepaidtotal_fyb = Column(Numeric)
    mainaccountcode = Column(Text)
    objectclass = Column(Text)
    obligationsdeliveredordersunpaidtotal_cpe = Column(Numeric)
    obligationsdeliveredordersunpaidtotal_fyb = Column(Numeric)
    obligationsincurredbyprogramobjectclass_cpe = Column(Numeric)
    obligationsundeliveredordersunpaidtotal_cpe = Column(Numeric)
    obligationsundeliveredordersunpaidtotal_fyb = Column(Numeric)
    programactivitycode = Column(Text)
    programactivityname = Column(Text)
    subaccountcode = Column(Text)
    ussgl480100_undeliveredordersobligationsunpaid_cpe = Column(Numeric)
    ussgl480100_undeliveredordersobligationsunpaid_fyb = Column(Numeric)
    ussgl480200_undeliveredordersobligationsprepaidadvanced_cpe = Column(
        "ussgl480200_undeliveredordersobligationsprepaidadv_cpe", Numeric)
    ussgl480200_undeliveredordersobligationsprepaidadvanced_fyb = Column(
        "ussgl480200_undeliveredordersobligationsprepaidadv_fyb", Numeric)
    ussgl483100_undeliveredordersobligationstransferredunpaid_cpe = Column(
        "ussgl483100_undeliveredordersobligtransferredunpaid_cpe", Numeric)
    ussgl483200_undeliveredordersobligationstransferredprepaidadvanced_cpe = Column(
        "ussgl483200_undeliveredordersobligtransferredppdadv_cpe", Numeric)
    ussgl487100_downwardadjustmentsofprioryearunpaidundeliveredordersobligationsrecoveries_cpe = Column(
        "ussgl487100_downadjsprioryrunpaidundelivordersobligrec_cpe", Numeric)
    ussgl487200_downwardadjustmentsofprioryearprepaidadvancedundeliveredordersobligationsrefundscollected_cpe = Column(
        "ussgl487200_downadjsprioryrppdadvundelivordersobligref_cpe", Numeric)
    ussgl488100_upwardadjustmentsofprioryearundeliveredordersobligationsunpaid_cpe = Column(
        "ussgl488100_upadjsprioryearundelivordersobligunpaid_cpe", Numeric)
    ussgl488200_upwardadjustmentsofprioryearundeliveredordersobligationsprepaidadvanced_cpe = Column(
        "ussgl488200_upadjsprioryrundelivordersobligprepaidadv_cpe", Numeric)
    ussgl490100_deliveredordersobligationsunpaid_cpe = Column(Numeric)
    ussgl490100_deliveredordersobligationsunpaid_fyb = Column(Numeric)
    ussgl490200_deliveredordersobligationspaid_cpe = Column(Numeric)
    ussgl490800_authorityoutlayednotyetdisbursed_cpe = Column(Numeric)
    ussgl490800_authorityoutlayednotyetdisbursed_fyb = Column(Numeric)
    ussgl493100_deliveredordersobligationstransferredunpaid_cpe = Column(
        "ussgl493100_deliveredordersobligstransferredunpaid_cpe", Numeric)
    ussgl497100_downwardadjustmentsofprioryearunpaiddeliveredordersobligationsrecoveries_cpe = Column(
        "ussgl497100_downadjsprioryrunpaiddelivordersobligrec_cpe", Numeric)
    ussgl497200_downwardadjustmentsofprioryearpaiddeliveredordersobligationsrefundscollected_cpe = Column(
        "ussgl497200_downadjsprioryrpaiddelivordersobligrefclt_cpe", Numeric)
    ussgl498100_upwardadjustmentsofprioryeardeliveredordersobligationsunpaid_cpe = Column(
        "ussgl498100_upadjsprioryeardeliveredordersobligunpaid_cpe", Numeric)
    ussgl498200_upwardadjustmentsofprioryeardeliveredordersobligationspaid_cpe = Column(
        "ussgl498200_upadjsprioryrdelivordersobligpaid_cpe", Numeric)
    tas = Column(Text, nullable=False, default=concatTas, onupdate=concatTas)
    valid_record = Column(Boolean, nullable = False, default = True, server_default = "True")

    def __init__(self, **kwargs):
        # broker is set up to ignore extra columns in submitted data
        # so get rid of any extraneous kwargs before instantiating
        cleanKwargs = {k: v for k, v in kwargs.items() if hasattr(self, k)}
        super(ObjectClassProgramActivity, self).__init__(**cleanKwargs)

Index("ix_oc_pa_tas_oc_pa",
      ObjectClassProgramActivity.tas,
      ObjectClassProgramActivity.objectclass,
      ObjectClassProgramActivity.programactivitycode,
      unique=False)

class AwardFinancial(Base):
    """Model for the award_financial table."""
    __tablename__ = "award_financial"

    award_financial_id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, nullable=False, index=True)
    job_id = Column(Integer, nullable=False, index=True)
    row_number = Column(Integer, nullable=False)
    agencyidentifier = Column(Text)
    allocationtransferagencyidentifier = Column(Text)
    availabilitytypecode = Column(Text)
    beginningperiodofavailability = Column(Text)
    bydirectreimbursablefundingsource = Column(Text)
    deobligationsrecoveriesrefundsofprioryearbyaward_cpe = Column(Numeric)
    endingperiodofavailability = Column(Text)
    fain = Column(Text, index=True)
    grossoutlayamountbyaward_cpe = Column(Numeric)
    grossoutlayamountbyaward_fyb = Column(Numeric)
    grossoutlaysdeliveredorderspaidtotal_cpe = Column(Numeric)
    grossoutlaysdeliveredorderspaidtotal_fyb = Column(Numeric)
    grossoutlaysundeliveredordersprepaidtotal_cpe = Column(Numeric)
    grossoutlaysundeliveredordersprepaidtotal_fyb = Column(Numeric)
    mainaccountcode = Column(Text)
    objectclass = Column(Text)
    obligationsdeliveredordersunpaidtotal_cpe = Column(Numeric)
    obligationsdeliveredordersunpaidtotal_fyb = Column(Numeric)
    obligationsincurredtotalbyaward_cpe = Column(Numeric)
    obligationsundeliveredordersunpaidtotal_cpe = Column(Numeric)
    obligationsundeliveredordersunpaidtotal_fyb = Column(Numeric)
    parentawardid = Column(Text)
    piid = Column(Text, index=True)
    programactivitycode = Column(Text)
    programactivityname = Column(Text)
    subaccountcode = Column(Text)
    transactionobligatedamount = Column(Numeric)
    uri = Column(Text, index=True)
    ussgl480100_undeliveredordersobligationsunpaid_cpe = Column(Numeric)
    ussgl480100_undeliveredordersobligationsunpaid_fyb = Column(Numeric)
    ussgl480200_undeliveredordersobligationsprepaidadvanced_cpe = Column(
        "ussgl480200_undeliveredordersobligationsprepaidadv_cpe", Numeric)
    ussgl480200_undeliveredordersobligationsprepaidadvanced_fyb = Column(Numeric)
    ussgl483100_undeliveredordersobligationstransferredunpaid_cpe = Column(
        "ussgl483100_undeliveredordersobligtransferredunpaid_cpe", Numeric)
    ussgl483200_undeliveredordersobligationstransferredprepaidadvanced_cpe = Column(
        "ussgl483200_undeliveredordersobligtransferredppdadv_cpe", Numeric)
    ussgl487100_downwardadjustmentsofprioryearunpaidundeliveredordersobligationsrecoveries_cpe = Column(
        "ussgl487100_downadjsprioryrunpaidundelivordersobligrec_cpe", Numeric)
    ussgl487200_downwardadjustmentsofprioryearprepaidadvancedundeliveredordersobligationsrefundscollected_cpe = Column(
        "ussgl487200_downadjsprioryrppdadvundelivordersobligref_cpe", Numeric)
    ussgl488100_upwardadjustmentsofprioryearundeliveredordersobligationsunpaid_cpe = Column(
        "ussgl488100_upadjsprioryearundelivordersobligunpaid_cpe", Numeric)
    ussgl488200_upwardadjustmentsofprioryearundeliveredordersobligationsprepaidadvanced_cpe = Column(
        "ussgl488200_upadjsprioryrundelivordersobligprepaidadv_cpe", Numeric)
    ussgl490100_deliveredordersobligationsunpaid_cpe = Column(Numeric)
    ussgl490100_deliveredordersobligationsunpaid_fyb = Column(Numeric)
    ussgl490200_deliveredordersobligationspaid_cpe = Column(Numeric)
    ussgl490800_authorityoutlayednotyetdisbursed_cpe = Column(Numeric)
    ussgl490800_authorityoutlayednotyetdisbursed_fyb = Column(Numeric)
    ussgl493100_deliveredordersobligationstransferredunpaid_cpe = Column(
        "ussgl493100_deliveredordersobligstransferredunpaid_cpe", Numeric)
    ussgl497100_downwardadjustmentsofprioryearunpaiddeliveredordersobligationsrecoveries_cpe = Column(
        "ussgl497100_downadjsprioryrunpaiddelivordersobligrec_cpe", Numeric)
    ussgl497200_downwardadjustmentsofprioryearpaiddeliveredordersobligationsrefundscollected_cpe = Column(
        "ussgl497200_downadjsprioryrpaiddelivordersobligrefclt_cpe", Numeric)
    ussgl498100_upwardadjustmentsofprioryeardeliveredordersobligationsunpaid_cpe  = Column(
        "ussgl498100_upadjsprioryeardeliveredordersobligunpaid_cpe", Numeric)
    ussgl498200_upwardadjustmentsofprioryeardeliveredordersobligationspaid_cpe = Column(
        "ussgl498200_upadjsprioryrdelivordersobligpaid_cpe", Numeric)
    tas = Column(Text, nullable=False, default=concatTas, onupdate=concatTas)
    valid_record = Column(Boolean, nullable = False, default = True, server_default = "True")

    def __init__(self, **kwargs):
        # broker is set up to ignore extra columns in submitted data
        # so get rid of any extraneous kwargs before instantiating
        cleanKwargs = {k: v for k, v in kwargs.items() if hasattr(self, k)}
        super(AwardFinancial, self).__init__(**cleanKwargs)

Index("ix_award_financial_tas_oc_pa",
      AwardFinancial.tas,
      AwardFinancial.objectclass,
      AwardFinancial.programactivitycode,
      unique=False)

class AwardFinancialAssistance(Base):
    """Model for the award_financial_assistance table."""
    __tablename__ = "award_financial_assistance"

    award_financial_assistance_id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, nullable=False, index=True)
    job_id = Column(Integer, nullable=False, index=True)
    row_number = Column(Integer, nullable=False)
    actiondate = Column(Text)
    actiontype = Column(Text)
    assistancetype = Column(Text)
    awarddescription = Column(Text)
    awardeeorrecipientlegalentityname = Column(Text)
    awardeeorrecipientuniqueidentifier = Column(Text)
    awardingagencycode = Column(Text)
    awardingagencyname = Column(Text)
    awardingofficecode = Column(Text)
    awardingofficename = Column(Text)
    awardingsubtieragencycode = Column(Text)
    awardingsubtieragencyname = Column(Text)
    awardmodificationamendmentnumber = Column(Text)
    businessfundsindicator = Column(Text)
    businesstypes = Column(Text)
    cfda_number = Column(Text)
    cfda_title = Column(Text)
    correctionlatedeleteindicator = Column(Text)
    facevalueloanguarantee = Column(Numeric)
    fain = Column(Text, index=True)
    federalactionobligation = Column(Numeric)
    fiscalyearandquartercorrection = Column(Text)
    fundingagencycode = Column(Text)
    fundingagencyname = Column(Text)
    fundingagencyofficename = Column(Text)
    fundingofficecode = Column(Text)
    fundingsubtieragencycode = Column(Text)
    fundingsubtieragencyname = Column(Text)
    legalentityaddressline1 = Column(Text)
    legalentityaddressline2 = Column(Text)
    legalentityaddressline3 = Column(Text)
    legalentitycitycode = Column(Text)
    legalentitycityname = Column(Text)
    legalentitycongressionaldistrict = Column(Text)
    legalentitycountrycode = Column(Text)
    legalentitycountycode = Column(Text)
    legalentitycountyname = Column(Text)
    legalentityforeigncityname = Column(Text)
    legalentityforeignpostalcode = Column(Text)
    legalentityforeignprovincename = Column(Text)
    legalentitystatecode = Column(Text)
    legalentitystatename = Column(Text)
    legalentityzip5 = Column(Text)
    legalentityziplast4 = Column(Text)
    nonfederalfundingamount = Column(Numeric)
    originalloansubsidycost = Column(Numeric)
    periodofperformancecurrentenddate = Column(Text)
    periodofperformancestartdate = Column(Text)
    primaryplaceofperformancecityname = Column(Text)
    primaryplaceofperformancecode = Column(Text)
    primaryplaceofperformancecongressionaldistrict = Column(Text)
    primaryplaceofperformancecountrycode = Column(Text)
    primaryplaceofperformancecountyname = Column(Text)
    primaryplaceofperformanceforeignlocationdescription = Column(Text)
    primaryplaceofperformancestatename = Column(Text)
    primaryplaceofperformancezipplus4 = Column(Text)
    recordtype = Column(Integer)
    sai_number = Column(Text)
    totalfundingamount = Column(Numeric)
    uri = Column(Text, index=True)
    valid_record = Column(Boolean, nullable = False, default = True, server_default = "True")

    def __init__(self, **kwargs):
        # broker is set up to ignore extra columns in submitted data
        # so get rid of any extraneous kwargs before instantiating
        cleanKwargs = {k: v for k, v in kwargs.items() if hasattr(self, k)}
        super(AwardFinancialAssistance, self).__init__(**cleanKwargs)
