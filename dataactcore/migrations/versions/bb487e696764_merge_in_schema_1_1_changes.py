"""merge in schema 1.1 changes

Revision ID: bb487e696764
Revises: 2ae156c8f46d, 9960bbbe4d92
Create Date: 2017-09-12 12:44:18.378324

"""

# revision identifiers, used by Alembic.
revision = 'bb487e696764'
down_revision = ('2ae156c8f46d', '9960bbbe4d92')
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

