""" Dropping createddate column from Office

Revision ID: ee5feab1e6bc
Revises: e11b9f18c851
Create Date: 2024-06-28 18:01:21.557584

"""

# revision identifiers, used by Alembic.
revision = 'ee5feab1e6bc'
down_revision = 'e11b9f18c851'
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
    op.drop_column('office', 'created_date')
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('office', sa.Column('created_date', sa.DATE(), autoincrement=False, nullable=True))
    # ### end Alembic commands ###
