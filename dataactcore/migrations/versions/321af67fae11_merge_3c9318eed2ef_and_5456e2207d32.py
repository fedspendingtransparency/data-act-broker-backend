"""merge 3c9318eed2ef and 5456e2207d32

Revision ID: 321af67fae11
Revises: 3c9318eed2ef, 5456e2207d32
Create Date: 2018-05-16 11:55:38.482967

"""

# revision identifiers, used by Alembic.
revision = '321af67fae11'
down_revision = ('3c9318eed2ef', '5456e2207d32')
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

