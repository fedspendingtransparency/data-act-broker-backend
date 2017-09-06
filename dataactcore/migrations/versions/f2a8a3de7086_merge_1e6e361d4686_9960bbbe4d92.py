"""merge 1e6e361d4686 9960bbbe4d92

Revision ID: f2a8a3de7086
Revises: 1e6e361d4686, 9960bbbe4d92
Create Date: 2017-09-06 16:21:56.754923

"""

# revision identifiers, used by Alembic.
revision = 'f2a8a3de7086'
down_revision = ('1e6e361d4686', '9960bbbe4d92')
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

