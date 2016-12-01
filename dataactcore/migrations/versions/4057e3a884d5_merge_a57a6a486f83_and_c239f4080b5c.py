"""merge a57a6a486f83 and c239f4080b5c

Revision ID: 4057e3a884d5
Revises: a57a6a486f83, c239f4080b5c
Create Date: 2016-11-16 13:28:07.589811

"""

# revision identifiers, used by Alembic.
revision = '4057e3a884d5'
down_revision = ('a57a6a486f83', 'c239f4080b5c')
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

