""" Merge d10d998b796b and d45dde2ba15b

Revision ID: 85858c8aaac5
Revises: d10d998b796b, d45dde2ba15b
Create Date: 2018-03-22 12:28:05.061386

"""

# revision identifiers, used by Alembic.
revision = '85858c8aaac5'
down_revision = ('d10d998b796b', 'd45dde2ba15b')
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

