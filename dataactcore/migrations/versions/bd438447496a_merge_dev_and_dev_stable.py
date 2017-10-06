"""merge dev and dev-stable

Revision ID: bd438447496a
Revises: 539307ecadea, 6126ac2553e5
Create Date: 2017-09-20 13:05:37.854204

"""

# revision identifiers, used by Alembic.
revision = 'bd438447496a'
down_revision = ('539307ecadea', '6126ac2553e5')
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

