"""merge a72 and ea8

Revision ID: f613a378c886
Revises: a7249e2d8a1a, ea8fbaa044d7
Create Date: 2016-10-19 20:36:49.333971

"""

# revision identifiers, used by Alembic.
revision = 'f613a378c886'
down_revision = ('a7249e2d8a1a', 'ea8fbaa044d7')
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

