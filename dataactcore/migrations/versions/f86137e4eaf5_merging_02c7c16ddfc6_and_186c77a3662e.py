"""Merging 02c7c16ddfc6 and 186c77a3662e

Revision ID: f86137e4eaf5
Revises: 02c7c16ddfc6, 186c77a3662e
Create Date: 2026-08-03 19:42:59.760783

"""

# revision identifiers, used by Alembic.
revision = 'f86137e4eaf5'
down_revision = ('02c7c16ddfc6', '186c77a3662e')
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

