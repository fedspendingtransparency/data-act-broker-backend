"""Merge 78edf5b0d088 963cad0fd72a

Revision ID: 0171eaaafaee
Revises: 78edf5b0d088, 963cad0fd72a
Create Date: 2016-12-09 19:18:15.313742

"""

# revision identifiers, used by Alembic.
revision = '0171eaaafaee'
down_revision = ('78edf5b0d088', '963cad0fd72a')
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

