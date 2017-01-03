"""Merge 6affb1acf439 and afff86382e26

Revision ID: f1022c7145d9
Revises: 6affb1acf439, afff86382e26
Create Date: 2017-01-03 15:22:31.169562

"""

# revision identifiers, used by Alembic.
revision = 'f1022c7145d9'
down_revision = ('6affb1acf439', 'afff86382e26')
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

