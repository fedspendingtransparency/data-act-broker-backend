"""merge 816e7367bbc9 and 7ffc95637ee4

Revision ID: 2cdb67bec1d3
Revises: 816e7367bbc9, 7ffc95637ee4
Create Date: 2018-06-29 11:09:30.753105

"""

# revision identifiers, used by Alembic.
revision = '2cdb67bec1d3'
down_revision = ('816e7367bbc9', '7ffc95637ee4')
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

