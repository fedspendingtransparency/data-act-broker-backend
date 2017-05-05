"""merge 403d00 805a9

Revision ID: 4ec514fa1f13
Revises: 403d00de3dce, 805a96ddd8c2
Create Date: 2017-05-01 11:03:18.169947

"""

# revision identifiers, used by Alembic.
revision = '4ec514fa1f13'
down_revision = ('403d00de3dce', '805a96ddd8c2')
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

