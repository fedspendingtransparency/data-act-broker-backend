"""Merge 1170d and b43b3

Revision ID: 8d66e1b5702d
Revises: 1170d628cf93, b43b300d718b
Create Date: 2019-01-31 14:49:15.031904

"""

# revision identifiers, used by Alembic.
revision = '8d66e1b5702d'
down_revision = ('1170d628cf93', 'b43b300d718b')
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

