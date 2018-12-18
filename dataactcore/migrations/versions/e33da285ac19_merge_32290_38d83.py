"""Merge 32290 and 38d83

Revision ID: e33da285ac19
Revises: 32290827d8fb, 38d8383270c1
Create Date: 2018-11-28 14:15:08.965645

"""

# revision identifiers, used by Alembic.
revision = 'e33da285ac19'
down_revision = ('32290827d8fb', '38d8383270c1')
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

