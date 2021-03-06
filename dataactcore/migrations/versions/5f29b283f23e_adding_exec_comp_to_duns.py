"""Adding exec comp to DUNS

Revision ID: 5f29b283f23e
Revises: e5b90e0b2ff8
Create Date: 2019-05-10 15:04:16.159511

"""

# revision identifiers, used by Alembic.
revision = '5f29b283f23e'
down_revision = 'e5b90e0b2ff8'
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
    op.add_column('duns', sa.Column('high_comp_officer1_amount', sa.Text(), nullable=True))
    op.add_column('duns', sa.Column('high_comp_officer1_full_na', sa.Text(), nullable=True))
    op.add_column('duns', sa.Column('high_comp_officer2_amount', sa.Text(), nullable=True))
    op.add_column('duns', sa.Column('high_comp_officer2_full_na', sa.Text(), nullable=True))
    op.add_column('duns', sa.Column('high_comp_officer3_amount', sa.Text(), nullable=True))
    op.add_column('duns', sa.Column('high_comp_officer3_full_na', sa.Text(), nullable=True))
    op.add_column('duns', sa.Column('high_comp_officer4_amount', sa.Text(), nullable=True))
    op.add_column('duns', sa.Column('high_comp_officer4_full_na', sa.Text(), nullable=True))
    op.add_column('duns', sa.Column('high_comp_officer5_amount', sa.Text(), nullable=True))
    op.add_column('duns', sa.Column('high_comp_officer5_full_na', sa.Text(), nullable=True))
    op.add_column('duns', sa.Column('last_exec_comp_mod_date', sa.Date(), nullable=True))
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('duns', 'high_comp_officer5_full_na')
    op.drop_column('duns', 'high_comp_officer5_amount')
    op.drop_column('duns', 'high_comp_officer4_full_na')
    op.drop_column('duns', 'high_comp_officer4_amount')
    op.drop_column('duns', 'high_comp_officer3_full_na')
    op.drop_column('duns', 'high_comp_officer3_amount')
    op.drop_column('duns', 'high_comp_officer2_full_na')
    op.drop_column('duns', 'high_comp_officer2_amount')
    op.drop_column('duns', 'high_comp_officer1_full_na')
    op.drop_column('duns', 'high_comp_officer1_amount')
    op.drop_column('duns', 'last_exec_comp_mod_date')
    # ### end Alembic commands ###

