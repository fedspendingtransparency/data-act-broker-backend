"""merge 2ce6 0fcf

Revision ID: 180c6e439a69
Revises: 2ce6d5fd51ce, 0fcf578896f3
Create Date: 2016-11-01 15:39:32.498062

"""

# revision identifiers, used by Alembic.
revision = '180c6e439a69'
down_revision = ('2ce6d5fd51ce', '0fcf578896f3')
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

