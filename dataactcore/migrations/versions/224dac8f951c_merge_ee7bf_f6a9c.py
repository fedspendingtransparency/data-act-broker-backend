"""Merge ee7bff1d660c and f6a9c7e6694b

Revision ID: 224dac8f951c
Revises: ee7bff1d660c, f6a9c7e6694b
Create Date: 2018-03-30 09:56:19.308323

"""

# revision identifiers, used by Alembic.
revision = '224dac8f951c'
down_revision = ('ee7bff1d660c', 'f6a9c7e6694b')
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

