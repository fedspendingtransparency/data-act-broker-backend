""" Merge 62b8cb9f8067 and d15d2cdfa25e

Revision ID: aa1b50bf0dfb
Revises: 62b8cb9f8067, d15d2cdfa25e
Create Date: 2023-04-25 18:12:35.543183

"""

# revision identifiers, used by Alembic.
revision = 'aa1b50bf0dfb'
down_revision = ('62b8cb9f8067', 'd15d2cdfa25e')
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

