"""Add icon_name to CGAC and FREC tables

Revision ID: e24af362f06a
Revises: 9e58ce58e4ee
Create Date: 2020-01-30 13:12:42.720743

"""

# revision identifiers, used by Alembic.
revision = 'e24af362f06a'
down_revision = '9e58ce58e4ee'
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
    op.add_column('cgac', sa.Column('icon_name', sa.Text(), nullable=True))
    op.add_column('frec', sa.Column('icon_name', sa.Text(), nullable=True))
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('frec', 'icon_name')
    op.drop_column('cgac', 'icon_name')
    # ### end Alembic commands ###

