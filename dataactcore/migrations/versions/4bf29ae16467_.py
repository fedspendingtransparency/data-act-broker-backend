"""Merge 456dee346fac and 465c323b7bf2

Revision ID: 4bf29ae16467
Revises: 456dee346fac, 465c323b7bf2
Create Date: 2017-01-11 10:13:22.750848

"""

# revision identifiers, used by Alembic.
revision = '4bf29ae16467'
down_revision = ('456dee346fac', '465c323b7bf2')
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

