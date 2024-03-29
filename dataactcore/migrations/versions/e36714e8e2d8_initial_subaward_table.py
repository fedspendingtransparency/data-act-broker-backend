""" Initial subaward table

Revision ID: e36714e8e2d8
Revises: 3f24399ddd1b
Create Date: 2019-05-28 20:29:57.312531

"""

# revision identifiers, used by Alembic.
revision = 'e36714e8e2d8'
down_revision = '3f24399ddd1b'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('subaward',
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('unique_award_key', sa.Text(), nullable=True),
    sa.Column('award_id', sa.Text(), nullable=True),
    sa.Column('parent_award_id', sa.Text(), nullable=True),
    sa.Column('award_amount', sa.Text(), nullable=True),
    sa.Column('action_date', sa.Text(), nullable=True),
    sa.Column('fy', sa.Text(), nullable=True),
    sa.Column('awarding_agency_code', sa.Text(), nullable=True),
    sa.Column('awarding_agency_name', sa.Text(), nullable=True),
    sa.Column('awarding_sub_tier_agency_c', sa.Text(), nullable=True),
    sa.Column('awarding_sub_tier_agency_n', sa.Text(), nullable=True),
    sa.Column('awarding_office_code', sa.Text(), nullable=True),
    sa.Column('awarding_office_name', sa.Text(), nullable=True),
    sa.Column('funding_agency_code', sa.Text(), nullable=True),
    sa.Column('funding_agency_name', sa.Text(), nullable=True),
    sa.Column('funding_sub_tier_agency_co', sa.Text(), nullable=True),
    sa.Column('funding_sub_tier_agency_na', sa.Text(), nullable=True),
    sa.Column('funding_office_code', sa.Text(), nullable=True),
    sa.Column('funding_office_name', sa.Text(), nullable=True),
    sa.Column('awardee_or_recipient_uniqu', sa.Text(), nullable=True),
    sa.Column('awardee_or_recipient_legal', sa.Text(), nullable=True),
    sa.Column('dba_name', sa.Text(), nullable=True),
    sa.Column('ultimate_parent_unique_ide', sa.Text(), nullable=True),
    sa.Column('ultimate_parent_legal_enti', sa.Text(), nullable=True),
    sa.Column('legal_entity_country_code', sa.Text(), nullable=True),
    sa.Column('legal_entity_country_name', sa.Text(), nullable=True),
    sa.Column('legal_entity_address_line1', sa.Text(), nullable=True),
    sa.Column('legal_entity_city_name', sa.Text(), nullable=True),
    sa.Column('legal_entity_state_code', sa.Text(), nullable=True),
    sa.Column('legal_entity_state_name', sa.Text(), nullable=True),
    sa.Column('legal_entity_zip', sa.Text(), nullable=True),
    sa.Column('legal_entity_congressional', sa.Text(), nullable=True),
    sa.Column('legal_entity_foreign_posta', sa.Text(), nullable=True),
    sa.Column('business_types', sa.Text(), nullable=True),
    sa.Column('place_of_perform_city_name', sa.Text(), nullable=True),
    sa.Column('place_of_perform_state_code', sa.Text(), nullable=True),
    sa.Column('place_of_perform_state_name', sa.Text(), nullable=True),
    sa.Column('place_of_performance_zip', sa.Text(), nullable=True),
    sa.Column('place_of_perform_congressio', sa.Text(), nullable=True),
    sa.Column('place_of_perform_country_co', sa.Text(), nullable=True),
    sa.Column('place_of_perform_country_na', sa.Text(), nullable=True),
    sa.Column('award_description', sa.Text(), nullable=True),
    sa.Column('naics', sa.Text(), nullable=True),
    sa.Column('naics_description', sa.Text(), nullable=True),
    sa.Column('cfda_numbers', sa.Text(), nullable=True),
    sa.Column('cfda_titles', sa.Text(), nullable=True),
    sa.Column('subaward_type', sa.Text(), nullable=True),
    sa.Column('subaward_report_year', sa.Text(), nullable=True),
    sa.Column('subaward_report_month', sa.Text(), nullable=True),
    sa.Column('subaward_number', sa.Text(), nullable=True),
    sa.Column('subaward_amount', sa.Text(), nullable=True),
    sa.Column('sub_action_date', sa.Text(), nullable=True),
    sa.Column('sub_awardee_or_recipient_uniqu', sa.Text(), nullable=True),
    sa.Column('sub_awardee_or_recipient_legal', sa.Text(), nullable=True),
    sa.Column('sub_dba_name', sa.Text(), nullable=True),
    sa.Column('sub_ultimate_parent_unique_ide', sa.Text(), nullable=True),
    sa.Column('sub_ultimate_parent_legal_enti', sa.Text(), nullable=True),
    sa.Column('sub_legal_entity_country_code', sa.Text(), nullable=True),
    sa.Column('sub_legal_entity_country_name', sa.Text(), nullable=True),
    sa.Column('sub_legal_entity_address_line1', sa.Text(), nullable=True),
    sa.Column('sub_legal_entity_city_name', sa.Text(), nullable=True),
    sa.Column('sub_legal_entity_state_code', sa.Text(), nullable=True),
    sa.Column('sub_legal_entity_state_name', sa.Text(), nullable=True),
    sa.Column('sub_legal_entity_zip', sa.Text(), nullable=True),
    sa.Column('sub_legal_entity_congressional', sa.Text(), nullable=True),
    sa.Column('sub_legal_entity_foreign_posta', sa.Text(), nullable=True),
    sa.Column('sub_business_types', sa.Text(), nullable=True),
    sa.Column('sub_place_of_perform_city_name', sa.Text(), nullable=True),
    sa.Column('sub_place_of_perform_state_code', sa.Text(), nullable=True),
    sa.Column('sub_place_of_perform_state_name', sa.Text(), nullable=True),
    sa.Column('sub_place_of_performance_zip', sa.Text(), nullable=True),
    sa.Column('sub_place_of_perform_congressio', sa.Text(), nullable=True),
    sa.Column('sub_place_of_perform_country_co', sa.Text(), nullable=True),
    sa.Column('sub_place_of_perform_country_na', sa.Text(), nullable=True),
    sa.Column('subaward_description', sa.Text(), nullable=True),
    sa.Column('sub_high_comp_officer1_full_na', sa.Text(), nullable=True),
    sa.Column('sub_high_comp_officer1_amount', sa.Text(), nullable=True),
    sa.Column('sub_high_comp_officer2_full_na', sa.Text(), nullable=True),
    sa.Column('sub_high_comp_officer2_amount', sa.Text(), nullable=True),
    sa.Column('sub_high_comp_officer3_full_na', sa.Text(), nullable=True),
    sa.Column('sub_high_comp_officer3_amount', sa.Text(), nullable=True),
    sa.Column('sub_high_comp_officer4_full_na', sa.Text(), nullable=True),
    sa.Column('sub_high_comp_officer4_amount', sa.Text(), nullable=True),
    sa.Column('sub_high_comp_officer5_full_na', sa.Text(), nullable=True),
    sa.Column('sub_high_comp_officer5_amount', sa.Text(), nullable=True),
    sa.Column('prime_id', sa.Integer(), nullable=True),
    sa.Column('internal_id', sa.Text(), nullable=True),
    sa.Column('date_submitted', sa.Text(), nullable=True),
    sa.Column('report_type', sa.Text(), nullable=True),
    sa.Column('transaction_type', sa.Text(), nullable=True),
    sa.Column('program_title', sa.Text(), nullable=True),
    sa.Column('contract_agency_code', sa.Text(), nullable=True),
    sa.Column('contract_idv_agency_code', sa.Text(), nullable=True),
    sa.Column('grant_funding_agency_id', sa.Text(), nullable=True),
    sa.Column('grant_funding_agency_name', sa.Text(), nullable=True),
    sa.Column('federal_agency_name', sa.Text(), nullable=True),
    sa.Column('treasury_symbol', sa.Text(), nullable=True),
    sa.Column('dunsplus4', sa.Text(), nullable=True),
    sa.Column('recovery_model_q1', sa.Text(), nullable=True),
    sa.Column('recovery_model_q2', sa.Text(), nullable=True),
    sa.Column('compensation_q1', sa.Text(), nullable=True),
    sa.Column('compensation_q2', sa.Text(), nullable=True),
    sa.Column('high_comp_officer1_full_na', sa.Text(), nullable=True),
    sa.Column('high_comp_officer1_amount', sa.Text(), nullable=True),
    sa.Column('high_comp_officer2_full_na', sa.Text(), nullable=True),
    sa.Column('high_comp_officer2_amount', sa.Text(), nullable=True),
    sa.Column('high_comp_officer3_full_na', sa.Text(), nullable=True),
    sa.Column('high_comp_officer3_amount', sa.Text(), nullable=True),
    sa.Column('high_comp_officer4_full_na', sa.Text(), nullable=True),
    sa.Column('high_comp_officer4_amount', sa.Text(), nullable=True),
    sa.Column('high_comp_officer5_full_na', sa.Text(), nullable=True),
    sa.Column('high_comp_officer5_amount', sa.Text(), nullable=True),
    sa.Column('sub_id', sa.Integer(), nullable=True),
    sa.Column('sub_parent_id', sa.Integer(), nullable=True),
    sa.Column('sub_federal_agency_id', sa.Text(), nullable=True),
    sa.Column('sub_federal_agency_name', sa.Text(), nullable=True),
    sa.Column('sub_funding_agency_id', sa.Text(), nullable=True),
    sa.Column('sub_funding_agency_name', sa.Text(), nullable=True),
    sa.Column('sub_funding_office_id', sa.Text(), nullable=True),
    sa.Column('sub_funding_office_name', sa.Text(), nullable=True),
    sa.Column('sub_naics', sa.Text(), nullable=True),
    sa.Column('sub_cfda_numbers', sa.Text(), nullable=True),
    sa.Column('sub_dunsplus4', sa.Text(), nullable=True),
    sa.Column('sub_recovery_subcontract_amt', sa.Text(), nullable=True),
    sa.Column('sub_recovery_model_q1', sa.Text(), nullable=True),
    sa.Column('sub_recovery_model_q2', sa.Text(), nullable=True),
    sa.Column('sub_compensation_q1', sa.Text(), nullable=True),
    sa.Column('sub_compensation_q2', sa.Text(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_subaward_action_date'), 'subaward', ['action_date'], unique=False)
    op.create_index(op.f('ix_subaward_award_id'), 'subaward', ['award_id'], unique=False)
    op.create_index(op.f('ix_subaward_awardee_or_recipient_uniqu'), 'subaward', ['awardee_or_recipient_uniqu'], unique=False)
    op.create_index(op.f('ix_subaward_awarding_agency_code'), 'subaward', ['awarding_agency_code'], unique=False)
    op.create_index(op.f('ix_subaward_awarding_sub_tier_agency_c'), 'subaward', ['awarding_sub_tier_agency_c'], unique=False)
    op.create_index(op.f('ix_subaward_funding_agency_code'), 'subaward', ['funding_agency_code'], unique=False)
    op.create_index(op.f('ix_subaward_funding_sub_tier_agency_co'), 'subaward', ['funding_sub_tier_agency_co'], unique=False)
    op.create_index(op.f('ix_subaward_internal_id'), 'subaward', ['internal_id'], unique=False)
    op.create_index(op.f('ix_subaward_parent_award_id'), 'subaward', ['parent_award_id'], unique=False)
    op.create_index(op.f('ix_subaward_prime_id'), 'subaward', ['prime_id'], unique=False)
    op.create_index(op.f('ix_subaward_sub_action_date'), 'subaward', ['sub_action_date'], unique=False)
    op.create_index(op.f('ix_subaward_sub_awardee_or_recipient_uniqu'), 'subaward', ['sub_awardee_or_recipient_uniqu'], unique=False)
    op.create_index(op.f('ix_subaward_sub_id'), 'subaward', ['sub_id'], unique=False)
    op.create_index(op.f('ix_subaward_sub_parent_id'), 'subaward', ['sub_parent_id'], unique=False)
    op.create_index(op.f('ix_subaward_subaward_number'), 'subaward', ['subaward_number'], unique=False)
    op.create_index(op.f('ix_subaward_subaward_type'), 'subaward', ['subaward_type'], unique=False)
    op.create_index(op.f('ix_subaward_unique_award_key'), 'subaward', ['unique_award_key'], unique=False)
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_subaward_unique_award_key'), table_name='subaward')
    op.drop_index(op.f('ix_subaward_subaward_type'), table_name='subaward')
    op.drop_index(op.f('ix_subaward_subaward_number'), table_name='subaward')
    op.drop_index(op.f('ix_subaward_sub_parent_id'), table_name='subaward')
    op.drop_index(op.f('ix_subaward_sub_id'), table_name='subaward')
    op.drop_index(op.f('ix_subaward_sub_awardee_or_recipient_uniqu'), table_name='subaward')
    op.drop_index(op.f('ix_subaward_sub_action_date'), table_name='subaward')
    op.drop_index(op.f('ix_subaward_prime_id'), table_name='subaward')
    op.drop_index(op.f('ix_subaward_parent_award_id'), table_name='subaward')
    op.drop_index(op.f('ix_subaward_internal_id'), table_name='subaward')
    op.drop_index(op.f('ix_subaward_funding_sub_tier_agency_co'), table_name='subaward')
    op.drop_index(op.f('ix_subaward_funding_agency_code'), table_name='subaward')
    op.drop_index(op.f('ix_subaward_awarding_sub_tier_agency_c'), table_name='subaward')
    op.drop_index(op.f('ix_subaward_awarding_agency_code'), table_name='subaward')
    op.drop_index(op.f('ix_subaward_awardee_or_recipient_uniqu'), table_name='subaward')
    op.drop_index(op.f('ix_subaward_award_id'), table_name='subaward')
    op.drop_index(op.f('ix_subaward_action_date'), table_name='subaward')
    op.drop_table('subaward')
    # ### end Alembic commands ###

