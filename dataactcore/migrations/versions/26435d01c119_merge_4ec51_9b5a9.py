"""merge 4ec51 9b5a9

Revision ID: 26435d01c119
Revises: 4ec514fa1f13, 9b5a9bfcf649
Create Date: 2017-05-08 08:13:17.791423

"""

# revision identifiers, used by Alembic.
revision = '26435d01c119'
down_revision = ('4ec514fa1f13', '9b5a9bfcf649')
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

