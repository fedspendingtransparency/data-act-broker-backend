"""Creating gtas_boc table

Revision ID: 6a7e61963da3
Revises: 7cb410fcdc60
Create Date: 2024-03-18 10:05:12.653277

"""

# revision identifiers, used by Alembic.
revision = '6a7e61963da3'
down_revision = '7cb410fcdc60'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('gtas_boc',
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.Column('gtas_boc_id', sa.Integer(), nullable=False),
    sa.Column('agency_identifier', sa.Text(), nullable=True),
    sa.Column('allocation_transfer_agency', sa.Text(), nullable=True),
    sa.Column('availability_type_code', sa.Text(), nullable=True),
    sa.Column('beginning_period_of_availa', sa.Text(), nullable=True),
    sa.Column('ending_period_of_availabil', sa.Text(), nullable=True),
    sa.Column('main_account_code', sa.Text(), nullable=False),
    sa.Column('sub_account_code', sa.Text(), nullable=False),
    sa.Column('tas', sa.Text(), nullable=False),
    sa.Column('display_tas', sa.Text(), nullable=True),
    sa.Column('fiscal_year', sa.Integer(), nullable=False),
    sa.Column('period', sa.Integer(), nullable=False),
    sa.Column('ussgl_number', sa.Text(), nullable=True),
    sa.Column('dollar_amount', sa.Numeric(), nullable=True),
    sa.Column('debit_credit', sa.Text(), nullable=True),
    sa.Column('begin_end', sa.Text(), nullable=True),
    sa.Column('authority_type', sa.Text(), nullable=True),
    sa.Column('reimbursable_flag', sa.Text(), nullable=True),
    sa.Column('apportionment_cat_code', sa.Text(), nullable=True),
    sa.Column('apportionment_cat_b_prog', sa.Text(), nullable=True),
    sa.Column('program_report_cat_number', sa.Text(), nullable=True),
    sa.Column('federal_nonfederal', sa.Text(), nullable=True),
    sa.Column('trading_partner_agency_ide', sa.Text(), nullable=True),
    sa.Column('trading_partner_mac', sa.Text(), nullable=True),
    sa.Column('year_of_budget_auth_code', sa.Text(), nullable=True),
    sa.Column('availability_time', sa.Text(), nullable=True),
    sa.Column('bea_category', sa.Text(), nullable=True),
    sa.Column('borrowing_source', sa.Text(), nullable=True),
    sa.Column('exchange_or_nonexchange', sa.Text(), nullable=True),
    sa.Column('custodial_noncustodial', sa.Text(), nullable=True),
    sa.Column('budget_impact', sa.Text(), nullable=True),
    sa.Column('prior_year_adjustment_code', sa.Text(), nullable=True),
    sa.Column('credit_cohort_year', sa.Integer(), nullable=True),
    sa.Column('disaster_emergency_fund_code', sa.Text(), nullable=True),
    sa.Column('reduction_type', sa.Text(), nullable=True),
    sa.Column('budget_object_class', sa.Text(), nullable=True),
    sa.PrimaryKeyConstraint('gtas_boc_id')
    )
    op.create_index(op.f('ix_gtas_boc_disaster_emergency_fund_code'), 'gtas_boc', ['disaster_emergency_fund_code'], unique=False)
    op.create_index(op.f('ix_gtas_boc_display_tas'), 'gtas_boc', ['display_tas'], unique=False)
    op.create_index(op.f('ix_gtas_boc_fiscal_year'), 'gtas_boc', ['fiscal_year'], unique=False)
    op.create_index(op.f('ix_gtas_boc_period'), 'gtas_boc', ['period'], unique=False)
    op.create_index(op.f('ix_gtas_boc_tas'), 'gtas_boc', ['tas'], unique=False)
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_gtas_boc_tas'), table_name='gtas_boc')
    op.drop_index(op.f('ix_gtas_boc_period'), table_name='gtas_boc')
    op.drop_index(op.f('ix_gtas_boc_fiscal_year'), table_name='gtas_boc')
    op.drop_index(op.f('ix_gtas_boc_display_tas'), table_name='gtas_boc')
    op.drop_index(op.f('ix_gtas_boc_disaster_emergency_fund_code'), table_name='gtas_boc')
    op.drop_table('gtas_boc')
    # ### end Alembic commands ###
