"""Merge cd1025a and e4ac552

Revision ID: 6126ac2553e5
Revises: cd1025ac9399, e4ac552148ef
Create Date: 2017-09-19 08:45:33.624295

"""

# revision identifiers, used by Alembic.
revision = '6126ac2553e5'
down_revision = ('cd1025ac9399', 'e4ac552148ef')
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

