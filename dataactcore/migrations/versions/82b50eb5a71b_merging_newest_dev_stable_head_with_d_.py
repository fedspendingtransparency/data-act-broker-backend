"""merging newest dev-stable head with d file gen head

Revision ID: 82b50eb5a71b
Revises: 1fc4844837cf, bb487e696764
Create Date: 2017-09-12 15:22:38.523640

"""

# revision identifiers, used by Alembic.
revision = '82b50eb5a71b'
down_revision = ('1fc4844837cf', 'bb487e696764')
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

