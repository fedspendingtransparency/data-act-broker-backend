"""empty message

Revision ID: 7d2d17f4e86a
Revises: de48ea53b63c, 65bcf3714cb5, 9199891101c5
Create Date: 2018-06-06 17:14:03.637816

"""

# revision identifiers, used by Alembic.
revision = '7d2d17f4e86a'
down_revision = ('de48ea53b63c', '9199891101c5')
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

