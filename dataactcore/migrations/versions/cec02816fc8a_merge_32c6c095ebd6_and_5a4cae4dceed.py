"""merge 32c6c095ebd6 and 5a4cae4dceed

Revision ID: cec02816fc8a
Revises: 32c6c095ebd6, 5a4cae4dceed
Create Date: 2017-05-24 12:55:34.990351

"""

# revision identifiers, used by Alembic.
revision = 'cec02816fc8a'
down_revision = ('32c6c095ebd6', '5a4cae4dceed')
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

