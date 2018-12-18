"""Merging 76d8a31caa51 and ab4eac43f605

Revision ID: 66ce64f4c1da
Revises: 76d8a31caa51, ab4eac43f605
Create Date: 2018-09-13 14:18:42.243393

"""

# revision identifiers, used by Alembic.
revision = '66ce64f4c1da'
down_revision = ('76d8a31caa51', 'ab4eac43f605')
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

