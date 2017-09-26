"""merge d file gen and dev-stable heads

Revision ID: a767facf8ea8
Revises: 82b50eb5a71b, b8bf72c05b0f
Create Date: 2017-09-18 14:31:47.179968

"""

# revision identifiers, used by Alembic.
revision = 'a767facf8ea8'
down_revision = ('82b50eb5a71b', 'b8bf72c05b0f')
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

