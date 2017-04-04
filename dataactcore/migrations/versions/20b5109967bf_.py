"""empty message

Revision ID: 20b5109967bf
Revises: 17ec44522729, d7e2e541f6d6
Create Date: 2017-04-03 11:25:27.381702

"""

# revision identifiers, used by Alembic.
revision = '20b5109967bf'
down_revision = ('17ec44522729', 'd7e2e541f6d6')
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

