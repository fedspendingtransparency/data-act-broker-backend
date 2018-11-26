"""empty message

Revision ID: 32290827d8fb
Revises: a9d1268b8b2a, 660c8c1a02e3
Create Date: 2018-11-26 15:54:39.929581

"""

# revision identifiers, used by Alembic.
revision = '32290827d8fb'
down_revision = ('a9d1268b8b2a', '660c8c1a02e3')
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

