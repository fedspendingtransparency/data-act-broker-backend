"""merge acdfe and aa10a

Revision ID: 3e3b37850d21
Revises: aa10ae595d3e, acdfe80a85d4
Create Date: 2017-06-26 22:45:27.301119

"""

# revision identifiers, used by Alembic.
revision = '3e3b37850d21'
down_revision = ('aa10ae595d3e', 'acdfe80a85d4')
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

