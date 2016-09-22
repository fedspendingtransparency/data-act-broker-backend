"""merge fsrs index with master

Revision ID: 51679e7bfe01
Revises: d074fd492181, dcbb5afa125e
Create Date: 2016-09-20 14:46:56.470399

"""

# revision identifiers, used by Alembic.
revision = '51679e7bfe01'
down_revision = ('d074fd492181', 'dcbb5afa125e')
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

