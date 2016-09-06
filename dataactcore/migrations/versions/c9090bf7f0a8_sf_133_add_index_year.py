"""sf-133-add-index-year

Revision ID: c9090bf7f0a8
Revises: 73db7d2cc754
Create Date: 2016-09-06 16:58:02.279630

"""

# revision identifiers, used by Alembic.
revision = 'c9090bf7f0a8'
down_revision = '73db7d2cc754'
branch_labels = None
depends_on = None

from alembic import op


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_data_broker():
    op.drop_index('ix_sf_133_tas', table_name='sf_133')
    op.create_index('ix_sf_133_tas', 'sf_133', ['tas', 'fiscal_year', 'period', 'line'], unique=True)


def downgrade_data_broker():
    op.drop_index('ix_sf_133_tas', table_name='sf_133')
    op.create_index('ix_sf_133_tas', 'sf_133', ['tas', 'period', 'line'], unique=True)

