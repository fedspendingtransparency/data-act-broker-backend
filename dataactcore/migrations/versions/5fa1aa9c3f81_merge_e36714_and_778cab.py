"""Merge e36714e8e2d8 and 778cabef7323

Revision ID: 5fa1aa9c3f81
Revises: e36714e8e2d8, 778cabef7323
Create Date: 2019-06-10 11:17:56.423433

"""

# revision identifiers, used by Alembic.
revision = '5fa1aa9c3f81'
down_revision = ('e36714e8e2d8', '778cabef7323')
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

