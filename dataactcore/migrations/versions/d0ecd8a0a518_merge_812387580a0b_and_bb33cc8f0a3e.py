"""merge 812387580a0b and bb33cc8f0a3e

Revision ID: d0ecd8a0a518
Revises: 812387580a0b, bb33cc8f0a3e
Create Date: 2016-11-13 20:36:00.541465

"""

# revision identifiers, used by Alembic.
revision = 'd0ecd8a0a518'
down_revision = ('812387580a0b', 'bb33cc8f0a3e')
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

