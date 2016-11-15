"""merge 6f79cf30f2d7 and d0ecd8a0a518

Revision ID: a57a6a486f83
Revises: 6f79cf30f2d7, d0ecd8a0a518
Create Date: 2016-11-15 10:52:44.489373

"""

# revision identifiers, used by Alembic.
revision = 'a57a6a486f83'
down_revision = ('6f79cf30f2d7', 'd0ecd8a0a518')
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

