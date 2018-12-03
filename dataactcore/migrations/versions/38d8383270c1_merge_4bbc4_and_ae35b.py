"""Merge 4bbc47f2b48d and ae35bd44ec9a

Revision ID: 38d8383270c1
Revises: 4bbc47f2b48d, ae35bd44ec9a
Create Date: 2018-10-19 10:07:46.992062

"""

# revision identifiers, used by Alembic.
revision = '38d8383270c1'
down_revision = ('4bbc47f2b48d', 'ae35bd44ec9a')
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_data_broker():
    pass


def downgrade_data_broker():
    pass

