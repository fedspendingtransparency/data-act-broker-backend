"""merge ada3c1420257 and bab396e50b1f

Revision ID: 967fbc28a6ef
Revises: ada3c1420257, bab396e50b1f
Create Date: 2017-08-07 11:48:14.479939

"""

# revision identifiers, used by Alembic.
revision = '967fbc28a6ef'
down_revision = ('ada3c1420257', 'bab396e50b1f')
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

