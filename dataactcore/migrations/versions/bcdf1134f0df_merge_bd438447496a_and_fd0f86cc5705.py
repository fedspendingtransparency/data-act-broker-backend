"""merge bd438447496a and fd0f86cc5705

Revision ID: bcdf1134f0df
Revises: bd438447496a, fd0f86cc5705
Create Date: 2017-09-27 11:14:01.763062

"""

# revision identifiers, used by Alembic.
revision = 'bcdf1134f0df'
down_revision = ('bd438447496a', 'fd0f86cc5705')
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

