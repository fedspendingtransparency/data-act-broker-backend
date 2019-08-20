""" merge 3244aa7611ef and 4be5e411246b

Revision ID: 827e8db6242e
Revises: 3244aa7611ef, 4be5e411246b
Create Date: 2019-08-09 19:07:55.751994

"""

# revision identifiers, used by Alembic.
revision = '827e8db6242e'
down_revision = ('3244aa7611ef', '4be5e411246b')
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

