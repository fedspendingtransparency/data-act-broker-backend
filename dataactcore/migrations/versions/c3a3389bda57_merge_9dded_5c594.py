"""Merge 9dded 5c594

Revision ID: c3a3389bda57
Revises: 9dded6e6bf79, 5c594d23709b
Create Date: 2016-11-04 15:10:00.221900

"""

# revision identifiers, used by Alembic.
revision = 'c3a3389bda57'
down_revision = ('9dded6e6bf79', '5c594d23709b')
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

