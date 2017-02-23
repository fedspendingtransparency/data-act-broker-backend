"""empty message

Revision ID: 684f82692765
Revises: 40f1074309d4, 9889ac822e9c
Create Date: 2017-02-22 15:37:37.497576

"""

# revision identifiers, used by Alembic.
revision = '684f82692765'
down_revision = ('40f1074309d4', '9889ac822e9c')
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

