"""empty message

Revision ID: c048f1dcdfa2
Revises: 9649eb55fa0b, 967fbc28a6ef
Create Date: 2017-08-08 13:57:48.590575

"""

# revision identifiers, used by Alembic.
revision = 'c048f1dcdfa2'
down_revision = ('9649eb55fa0b', '967fbc28a6ef')
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

