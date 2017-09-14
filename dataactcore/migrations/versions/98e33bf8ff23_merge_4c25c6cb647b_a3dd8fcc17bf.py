"""Merge 4c25c6cb647b and a3dd8fcc17bf

Revision ID: 98e33bf8ff23
Revises: 4c25c6cb647b, a3dd8fcc17bf
Create Date: 2017-01-26 09:31:54.506075

"""

# revision identifiers, used by Alembic.
revision = '98e33bf8ff23'
down_revision = ('4c25c6cb647b', 'a3dd8fcc17bf')
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

