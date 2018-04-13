"""Merge 1fabe and 7ac94

Revision ID: 7e54c1a1c341
Revises: 1fabe0bdd48c, 7ac94529d32e
Create Date: 2018-04-13 10:19:51.485977

"""

# revision identifiers, used by Alembic.
revision = '7e54c1a1c341'
down_revision = ('1fabe0bdd48c', '7ac94529d32e')
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

