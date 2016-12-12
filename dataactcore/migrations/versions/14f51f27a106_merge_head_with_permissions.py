"""Merge head with permissions

Revision ID: 14f51f27a106
Revises: 0171eaaafaee, 4260d9ddf25e
Create Date: 2016-12-12 18:28:29.801124

"""

# revision identifiers, used by Alembic.
revision = '14f51f27a106'
down_revision = ('0171eaaafaee', '4260d9ddf25e')
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

