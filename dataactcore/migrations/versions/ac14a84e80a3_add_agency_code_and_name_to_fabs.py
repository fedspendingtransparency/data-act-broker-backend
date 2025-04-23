"""empty message

Revision ID: ac14a84e80a3
Revises: 97955355bc51
Create Date: 2025-04-22 17:02:58.951709

"""

# revision identifiers, used by Alembic.
revision = 'ac14a84e80a3'
down_revision = '97955355bc51'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_data_broker():
    op.add_column('fabs', sa.Column('awarding_agency_code', sa.Text(), nullable=True))
    op.add_column('fabs', sa.Column('awarding_agency_name', sa.Text(), nullable=True))
    op.add_column('fabs', sa.Column('awarding_sub_tier_agency_n', sa.Text(), nullable=True))
    op.create_index(op.f('ix_fabs_awarding_agency_code'), 'fabs', ['awarding_agency_code'], unique=False)
    # ### end Alembic commands ###


def downgrade_data_broker():
    op.drop_index(op.f('ix_fabs_awarding_agency_code'), table_name='fabs')
    op.drop_column('fabs', 'awarding_sub_tier_agency_n')
    op.drop_column('fabs', 'awarding_agency_name')
    op.drop_column('fabs', 'awarding_agency_code')
    # ### end Alembic commands ###

