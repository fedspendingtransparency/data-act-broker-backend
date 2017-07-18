"""merge 9acf8cc4e454 and c42d328ef2fa

Revision ID: 5350b3e22c39
Revises: 9acf8cc4e454, c42d328ef2fa
Create Date: 2017-07-13 15:41:45.119582

"""

# revision identifiers, used by Alembic.
revision = '5350b3e22c39'
down_revision = ('9acf8cc4e454', 'c42d328ef2fa')
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

