"""Renaming daims_name to gsdm_name

Revision ID: 1b55916099f4
Revises: 6875b96c5ed3
Create Date: 2023-12-04 18:34:23.144177

"""

# revision identifiers, used by Alembic.
revision = '1b55916099f4'
down_revision = '6875b96c5ed3'
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
    op.add_column('file_columns', sa.Column('gsdm_name', sa.Text(), nullable=True))
    op.drop_column('file_columns', 'daims_name')
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('file_columns', sa.Column('daims_name', sa.TEXT(), autoincrement=False, nullable=True))
    op.drop_column('file_columns', 'gsdm_name')
    # ### end Alembic commands ###
