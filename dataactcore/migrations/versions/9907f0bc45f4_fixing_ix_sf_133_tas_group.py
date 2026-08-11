"""Fixing ix_sf_133_tas_group

Revision ID: 9907f0bc45f4
Revises: e10ecbfda17f
Create Date: 2026-08-04 17:18:41.025408

"""

# revision identifiers, used by Alembic.
revision = '9907f0bc45f4'
down_revision = 'e10ecbfda17f'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_data_broker():
    op.drop_index('ix_sf_133_tas_group', table_name='sf_133')
    op.create_index('ix_sf_133_tas_group', 'sf_133', ['tas', 'fiscal_year', 'period', 'line', 'disaster_emergency_fund_code', 'bea_category', 'budget_object_class', 'by_direct_reimbursable_fun', 'prior_year_adjustment', 'program_activity_reporting_key'], unique=True)


def downgrade_data_broker():
    op.drop_index('ix_sf_133_tas_group', table_name='sf_133')
    op.create_index('ix_sf_133_tas_group', 'sf_133', ['tas', 'fiscal_year', 'period', 'line', 'bea_category', 'budget_object_class', 'by_direct_reimbursable_fun', 'prior_year_adjustment', 'program_activity_reporting_key'], unique=True)
