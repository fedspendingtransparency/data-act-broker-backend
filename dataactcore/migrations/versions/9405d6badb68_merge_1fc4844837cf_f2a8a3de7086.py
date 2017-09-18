"""empty message

Revision ID: 9405d6badb68
Revises: 1fc4844837cf, f2a8a3de7086
Create Date: 2017-09-14 10:49:19.431450

"""

# revision identifiers, used by Alembic.
revision = '9405d6badb68'
down_revision = ('1fc4844837cf', 'f2a8a3de7086')
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

