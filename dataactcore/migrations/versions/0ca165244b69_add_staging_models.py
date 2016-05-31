"""add_staging_models

Revision ID: 0ca165244b69
Revises: 3a79d7a4d1c1
Create Date: 2016-05-31 14:33:30.072016

"""

# revision identifiers, used by Alembic.
revision = '0ca165244b69'
down_revision = '3a79d7a4d1c1'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_error_data():
    pass


def downgrade_error_data():
    pass


def upgrade_job_tracker():
    pass


def downgrade_job_tracker():
    pass


def upgrade_user_manager():
    pass


def downgrade_user_manager():
    pass


def upgrade_validation():
    pass


def downgrade_validation():
    pass


def upgrade_staging():
    # create appropriation table (file A)
    op.create_table('appropriation',
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.Column('appropriation_id', sa.Integer(), nullable=False),
    sa.Column('adjustmentstounobligatedbalancebroughtforward_cpe', sa.Numeric(), nullable=False),
    sa.Column('agencyidentifier', sa.Text(), nullable=False),
    sa.Column('allocationtransferagencyidentifier', sa.Text(), nullable=True),
    sa.Column('availabilitytypecode', sa.Text(), nullable=True),
    sa.Column('beginningperiodofavailability', sa.Text(), nullable=True),
    sa.Column('borrowingauthorityamounttotal_cpe', sa.Numeric(), nullable=True),
    sa.Column('budgetauthorityappropriatedamount_cpe', sa.Numeric(), nullable=False),
    sa.Column('budgetauthorityavailableamounttotal_cpe', sa.Numeric(), nullable=False),
    sa.Column('budgetauthorityunobligatedbalancebroughtforward_fyb', sa.Numeric(), nullable=True),
    sa.Column('contractauthorityamounttotal_cpe', sa.Numeric(), nullable=True),
    sa.Column('deobligationsrecoveriesrefundsbytas_cpe', sa.Numeric(), nullable=False),
    sa.Column('endingperiodofavailability', sa.Text(), nullable=True),
    sa.Column('grossoutlayamountbytas_cpe', sa.Numeric(), nullable=False),
    sa.Column('mainaccountcode', sa.Text(), nullable=False),
    sa.Column('obligationsincurredtotalbytas_cpe', sa.Numeric(), nullable=False),
    sa.Column('otherbudgetaryresourcesamount_cpe', sa.Numeric(), nullable=True),
    sa.Column('spendingauthorityfromoffsettingcollectionsamounttotal_cpe', sa.Numeric(), nullable=True),
    sa.Column('statusofbudgetaryresourcestotal_cpe', sa.Numeric(), nullable=False),
    sa.Column('subaccountcode', sa.Text(), nullable=False),
    sa.Column('unobligatedbalance_cpe', sa.Numeric(), nullable=False),
    sa.Column('tas', sa.Text(), nullable=False),
    sa.PrimaryKeyConstraint('appropriation_id')
    )
    op.create_index(op.f('ix_appropriation_tas'), 'appropriation', ['tas'], unique=False)

    # create object_class_program_activity table (file B)
    op.create_table('object_class_program_activity',
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.Column('object_class_program_activity_id', sa.Integer(), nullable=False),
    sa.Column('agencyidentifier', sa.Text(), nullable=False),
    sa.Column('allocationtransferagencyidentifier', sa.Text(), nullable=True),
    sa.Column('availabilitytypecode', sa.Text(), nullable=True),
    sa.Column('beginningperiodofavailability', sa.Text(), nullable=True),
    sa.Column('bydirectreimbursablefundingsource', sa.Text(), nullable=False),
    sa.Column('deobligationsrecoveriesrefundsprioryrbyprogobjectclass_cpe', sa.Numeric(),
              nullable=False),
    sa.Column('endingperiodofavailability', sa.Text(), nullable=True),
    sa.Column('grossoutlayamountbyprogramobjectclass_cpe', sa.Numeric(), nullable=False),
    sa.Column('grossoutlayamountbyprogramobjectclass_fyb', sa.Numeric(), nullable=False),
    sa.Column('grossoutlaysdeliveredorderspaidtotal_cpe', sa.Numeric(), nullable=False),
    sa.Column('grossoutlaysdeliveredorderspaidtotal_fyb', sa.Numeric(), nullable=False),
    sa.Column('grossoutlaysundeliveredordersprepaidtotal_cpe', sa.Numeric(), nullable=False),
    sa.Column('grossoutlaysundeliveredordersprepaidtotal_fyb', sa.Numeric(), nullable=False),
    sa.Column('mainaccountcode', sa.Text(), nullable=False),
    sa.Column('objectclass', sa.Text(), nullable=False),
    sa.Column('obligationsdeliveredordersunpaidtotal_cpe', sa.Numeric(), nullable=False),
    sa.Column('obligationsdeliveredordersunpaidtotal_fyb', sa.Numeric(), nullable=False),
    sa.Column('obligationsincurredbyprogramobjectclass_cpe', sa.Numeric(), nullable=False),
    sa.Column('obligationsundeliveredordersunpaidtotal_cpe', sa.Numeric(), nullable=False),
    sa.Column('obligationsundeliveredordersunpaidtotal_fyb', sa.Numeric(), nullable=False),
    sa.Column('programactivitycode', sa.Text(), nullable=False),
    sa.Column('programactivityname', sa.Text(), nullable=False),
    sa.Column('subaccountcode', sa.Text(), nullable=False),
    sa.Column('ussgl480100_undeliveredordersobligationsunpaid_cpe', sa.Numeric(), nullable=False),
    sa.Column('ussgl480100_undeliveredordersobligationsunpaid_fyb', sa.Numeric(), nullable=False),
    sa.Column('ussgl480200_undeliveredordersobligationsprepaidadv_cpe', sa.Numeric(), nullable=False),
    sa.Column('ussgl480200_undeliveredordersobligationsprepaidadv_fyb', sa.Numeric(), nullable=False),
    sa.Column('ussgl483100_undeliveredordersobligtransferredunpaid_cpe', sa.Numeric(), nullable=False),
    sa.Column('ussgl483200_undeliveredordersobligtransferredppdadv_cpe', sa.Numeric(), nullable=False),
    sa.Column('ussgl487100_downadjsprioryrunpaidundelivordersobligrec_cpe', sa.Numeric(),
              nullable=False),
    sa.Column('ussgl487200_downadjsprioryrppdadvundelivordersobligref_cpe', sa.Numeric(),
              nullable=False),
    sa.Column('ussgl488100_upadjsprioryearundelivordersobligunpaid_cpe', sa.Numeric(), nullable=False),
    sa.Column('ussgl488200_upadjsprioryrundelivordersobligprepaidadv_cpe', sa.Numeric(),
              nullable=False),
    sa.Column('ussgl490100_deliveredordersobligationsunpaid_cpe', sa.Numeric(), nullable=False),
    sa.Column('ussgl490100_deliveredordersobligationsunpaid_fyb', sa.Numeric(), nullable=False),
    sa.Column('ussgl490200_deliveredordersobligationspaid_cpe', sa.Numeric(), nullable=False),
    sa.Column('ussgl490800_authorityoutlayednotyetdisbursed_cpe', sa.Numeric(), nullable=False),
    sa.Column('ussgl490800_authorityoutlayednotyetdisbursed_fyb', sa.Numeric(), nullable=False),
    sa.Column('ussgl493100_deliveredordersobligstransferredunpaid_cpe', sa.Numeric(), nullable=False),
    sa.Column('ussgl497100_downadjsprioryrunpaiddelivordersobligrec_cpe', sa.Numeric(), nullable=False),
    sa.Column('ussgl497200_downadjsprioryrpaiddelivordersobligrefclt_cpe', sa.Numeric(),
              nullable=False),
    sa.Column('ussgl498100_upadjsprioryeardeliveredordersobligunpaid_cpe', sa.Numeric(),
              nullable=False),
    sa.Column('ussgl498200_upadjsprioryrdelivordersobligpaid_cpe', sa.Numeric(), nullable=False),
    sa.Column('tas', sa.Text(), nullable=False),
    sa.PrimaryKeyConstraint('object_class_program_activity_id')
    )

    # add tas/object class/program activity code index
    op.create_index(op.f('ix_oc_pa_tas_oc_pa'), 'object_class_program_activity',
                    ['tas', 'objectclass', 'programactivitycode'], unique=False)

    # Create award_financial table (file C)
    op.create_table('award_financial',
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.Column('award_financial_id', sa.Integer(), nullable=False),
    sa.Column('agencyidentifier', sa.Text(), nullable=False),
    sa.Column('allocationtransferagencyidentifier', sa.Text(), nullable=True),
    sa.Column('availabilitytypecode', sa.Text(), nullable=True),
    sa.Column('beginningperiodofavailability', sa.Text(), nullable=True),
    sa.Column('bydirectreimbursablefundingsource', sa.Text(), nullable=True),
    sa.Column('deobligationsrecoveriesrefundsofprioryearbyaward_cpe', sa.Numeric(), nullable=True),
    sa.Column('endingperiodofavailability', sa.Text(), nullable=True),
    sa.Column('fain', sa.Text(), nullable=True),
    sa.Column('grossoutlayamountbyaward_cpe', sa.Numeric(), nullable=True),
    sa.Column('grossoutlayamountbyaward_fyb', sa.Numeric(), nullable=True),
    sa.Column('grossoutlaysdeliveredorderspaidtotal_cpe', sa.Numeric(), nullable=True),
    sa.Column('grossoutlaysdeliveredorderspaidtotal_fyb', sa.Numeric(), nullable=True),
    sa.Column('grossoutlaysundeliveredordersprepaidtotal_cpe', sa.Numeric(), nullable=True),
    sa.Column('grossoutlaysundeliveredordersprepaidtotal_fyb', sa.Numeric(), nullable=True),
    sa.Column('mainaccountcode', sa.Text(), nullable=False),
    sa.Column('objectclass', sa.Integer(), nullable=False),
    sa.Column('obligationsdeliveredordersunpaidtotal_cpe', sa.Numeric(), nullable=True),
    sa.Column('obligationsdeliveredordersunpaidtotal_fyb', sa.Numeric(), nullable=True),
    sa.Column('obligationsincurredtotalbyaward_cpe', sa.Numeric(), nullable=True),
    sa.Column('obligationsundeliveredordersunpaidtotal_cpe', sa.Numeric(), nullable=True),
    sa.Column('obligationsundeliveredordersunpaidtotal_fyb', sa.Numeric(), nullable=True),
    sa.Column('parentawardid', sa.Text(), nullable=True),
    sa.Column('piid', sa.Text(), nullable=True),
    sa.Column('programactivitycode', sa.Text(), nullable=True),
    sa.Column('programactivityname', sa.Text(), nullable=True),
    sa.Column('subaccountcode', sa.Text(), nullable=False),
    sa.Column('transactionobligatedamount', sa.Numeric(), nullable=True),
    sa.Column('uri', sa.Text(), nullable=True),
    sa.Column('ussgl480100_undeliveredordersobligationsunpaid_cpe', sa.Numeric(), nullable=True),
    sa.Column('ussgl480100_undeliveredordersobligationsunpaid_fyb', sa.Numeric(), nullable=True),
    sa.Column('ussgl480200_undeliveredordersobligationsprepaidadv_cpe', sa.Numeric(), nullable=True),
    sa.Column('ussgl480200_undeliveredordersobligationsprepaidadvanced_fyb', sa.Numeric(), nullable=True),
    sa.Column('ussgl483100_undeliveredordersobligtransferredunpaid_cpe', sa.Numeric(), nullable=True),
    sa.Column('ussgl483200_undeliveredordersobligtransferredppdadv_cpe', sa.Numeric(), nullable=True),
    sa.Column('ussgl487100_downadjsprioryrunpaidundelivordersobligrec_cpe', sa.Numeric(), nullable=True),
    sa.Column('ussgl487200_downadjsprioryrppdadvundelivordersobligref_cpe', sa.Numeric(), nullable=True),
    sa.Column('ussgl488100_upadjsprioryearundelivordersobligunpaid_cpe', sa.Numeric(), nullable=True),
    sa.Column('ussgl488200_upadjsprioryrundelivordersobligprepaidadv_cpe', sa.Numeric(), nullable=True),
    sa.Column('ussgl490100_deliveredordersobligationsunpaid_cpe', sa.Numeric(), nullable=True),
    sa.Column('ussgl490100_deliveredordersobligationsunpaid_fyb', sa.Numeric(), nullable=True),
    sa.Column('ussgl490200_deliveredordersobligationspaid_cpe', sa.Numeric(), nullable=True),
    sa.Column('ussgl490800_authorityoutlayednotyetdisbursed_cpe', sa.Numeric(), nullable=True),
    sa.Column('ussgl490800_authorityoutlayednotyetdisbursed_fyb', sa.Numeric(), nullable=True),
    sa.Column('ussgl493100_deliveredordersobligstransferredunpaid_cpe', sa.Numeric(), nullable=True),
    sa.Column('ussgl497100_downadjsprioryrunpaiddelivordersobligrec_cpe', sa.Numeric(), nullable=True),
    sa.Column('ussgl497200_downadjsprioryrpaiddelivordersobligrefclt_cpe', sa.Numeric(), nullable=True),
    sa.Column('ussgl498100_upadjsprioryeardeliveredordersobligunpaid_cpe', sa.Numeric(), nullable=True),
    sa.Column('ussgl498200_upadjsprioryrdelivordersobligpaid_cpe', sa.Numeric(), nullable=True),
    sa.Column('tas', sa.Text(), nullable=False),
    sa.PrimaryKeyConstraint('award_financial_id')
    )

    # add tas/object class/program activity code index
    op.create_index(op.f('ix_award_financial_tas_oc_pa'), 'award_financial',
                    ['tas', 'objectclass', 'programactivitycode'], unique=False),

    # create award_financial_assistance table (file D2)
    op.create_table('award_financial_assistance',
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.Column('award_financial_assistance_id', sa.Integer(), nullable=False),
    sa.Column('actiondate', sa.Text(), nullable=False),
    sa.Column('actiontype', sa.Text(), nullable=True),
    sa.Column('assistancetype', sa.Text(), nullable=False),
    sa.Column('awarddescription', sa.Text(), nullable=True),
    sa.Column('awardeeorrecipientlegalentityname', sa.Text(), nullable=False),
    sa.Column('awardeeorrecipientuniqueidentifier', sa.Text(), nullable=True),
    sa.Column('awardingagencycode', sa.Text(), nullable=True),
    sa.Column('awardingagencyname', sa.Text(), nullable=True),
    sa.Column('awardingofficecode', sa.Text(), nullable=True),
    sa.Column('awardingofficename', sa.Text(), nullable=True),
    sa.Column('awardingsubtieragencycode', sa.Text(), nullable=False),
    sa.Column('awardingsubtieragencyname', sa.Text(), nullable=True),
    sa.Column('awardmodificationamendmentnumber', sa.Text(), nullable=True),
    sa.Column('businessfundsindicator', sa.Text(), nullable=False),
    sa.Column('businesstypes', sa.Text(), nullable=False),
    sa.Column('cfda_number', sa.Text(), nullable=False),
    sa.Column('cfda_title', sa.Text(), nullable=True),
    sa.Column('correctionlatedeleteindicator', sa.Text(), nullable=True),
    sa.Column('facevalueloanguarantee', sa.Numeric(), nullable=True),
    sa.Column('fain', sa.Text(), nullable=True),
    sa.Column('federalactionobligation', sa.Numeric(), nullable=True),
    sa.Column('fiscalyearandquartercorrection', sa.Text(), nullable=True),
    sa.Column('fundingagencycode', sa.Text(), nullable=True),
    sa.Column('fundingagencyname', sa.Text(), nullable=True),
    sa.Column('fundingagencyofficename', sa.Text(), nullable=True),
    sa.Column('fundingofficecode', sa.Text(), nullable=True),
    sa.Column('fundingsubtieragencycode', sa.Text(), nullable=True),
    sa.Column('fundingsubtieragencyname', sa.Text(), nullable=True),
    sa.Column('legalentityaddressline1', sa.Text(), nullable=True),
    sa.Column('legalentityaddressline2', sa.Text(), nullable=True),
    sa.Column('legalentityaddressline3', sa.Text(), nullable=True),
    sa.Column('legalentitycitycode', sa.Text(), nullable=True),
    sa.Column('legalentitycityname', sa.Text(), nullable=True),
    sa.Column('legalentitycongressionaldistrict', sa.Text(), nullable=True),
    sa.Column('legalentitycountrycode', sa.Text(), nullable=True),
    sa.Column('legalentitycountycode', sa.Text(), nullable=True),
    sa.Column('legalentitycountyname', sa.Text(), nullable=True),
    sa.Column('legalentityforeigncityname', sa.Text(), nullable=True),
    sa.Column('legalentityforeignpostalcode', sa.Text(), nullable=True),
    sa.Column('legalentityforeignprovincename', sa.Text(), nullable=True),
    sa.Column('legalentitystatecode', sa.Text(), nullable=True),
    sa.Column('legalentitystatename', sa.Text(), nullable=True),
    sa.Column('legalentityzip5', sa.Text(), nullable=True),
    sa.Column('legalentityziplast4', sa.Text(), nullable=True),
    sa.Column('nonfederalfundingamount', sa.Numeric(), nullable=True),
    sa.Column('originalloansubsidycost', sa.Numeric(), nullable=True),
    sa.Column('periodofperformancecurrentenddate', sa.Text(), nullable=True),
    sa.Column('periodofperformancestartdate', sa.Text(), nullable=True),
    sa.Column('primaryplaceofperformancecityname', sa.Text(), nullable=True),
    sa.Column('primaryplaceofperformancecode', sa.Text(), nullable=True),
    sa.Column('primaryplaceofperformancecongressionaldistrict', sa.Text(), nullable=True),
    sa.Column('primaryplaceofperformancecountrycode', sa.Text(), nullable=True),
    sa.Column('primaryplaceofperformancecountyname', sa.Text(), nullable=True),
    sa.Column('primaryplaceofperformanceforeignlocationdescription', sa.Text(), nullable=True),
    sa.Column('primaryplaceofperformancestatename', sa.Text(), nullable=True),
    sa.Column('primaryplaceofperformancezipplus4', sa.Text(), nullable=True),
    sa.Column('recordtype', sa.Integer(), nullable=True),
    sa.Column('sai_number', sa.Text(), nullable=True),
    sa.Column('totalfundingamount', sa.Numeric(), nullable=True),
    sa.Column('uri', sa.Text(), nullable=True),
    sa.PrimaryKeyConstraint('award_financial_assistance_id')
    )


def downgrade_staging():

    op.drop_table('award_financial_assistance')
    op.drop_table('award_financial')
    op.drop_table('object_class_program_activity')
    #op.drop_index(op.f('ix_appropriation_tas'), table_name='appropriation')
    op.drop_table('appropriation')

