""" Adding city_code to cd_city_grouped

Revision ID: 6875b96c5ed3
Revises: bd0657fb1950
Create Date: 2023-11-30 18:00:50.185607

"""

# revision identifiers, used by Alembic.
revision = '6875b96c5ed3'
down_revision = 'bd0657fb1950'
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
    op.add_column('cd_city_grouped', sa.Column('city_code', sa.Text(), nullable=True))
    op.create_index(op.f('ix_cd_city_grouped_city_code'), 'cd_city_grouped', ['city_code'], unique=False)
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_cd_city_grouped_city_code'), table_name='cd_city_grouped')
    op.drop_column('cd_city_grouped', 'city_code')
    # ### end Alembic commands ###
