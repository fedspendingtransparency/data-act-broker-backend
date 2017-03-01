"""empty message

Revision ID: 5cf1e89303a8
Revises: 101a8199cc94, 56f78c0fc729
Create Date: 2017-03-01 08:54:01.707349

"""

# revision identifiers, used by Alembic.
revision = '5cf1e89303a8'
down_revision = ('101a8199cc94', '56f78c0fc729')
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

