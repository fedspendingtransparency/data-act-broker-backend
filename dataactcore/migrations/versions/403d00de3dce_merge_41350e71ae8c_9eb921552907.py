"""Merge 41350e71ae8c 9eb921552907

Revision ID: 403d00de3dce
Revises: 41350e71ae8c, 9eb921552907
Create Date: 2017-04-20 12:15:27.258654

"""

# revision identifiers, used by Alembic.
revision = '403d00de3dce'
down_revision = ('41350e71ae8c', '9eb921552907')
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

