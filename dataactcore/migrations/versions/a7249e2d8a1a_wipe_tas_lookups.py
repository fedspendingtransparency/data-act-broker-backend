"""Wipe TAS lookups: one-off data migration to delete existing TASLookups

Revision ID: a7249e2d8a1a
Revises: 0c857b50962a
Create Date: 2016-10-18 19:58:19.837713

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'a7249e2d8a1a'
down_revision = '0c857b50962a'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_data_broker():
    op.execute('DELETE FROM tas_lookup')


def downgrade_data_broker():
    pass
