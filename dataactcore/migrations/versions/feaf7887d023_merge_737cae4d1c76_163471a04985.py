""" Merge 737cae4d1c76 and 163471a04985

Revision ID: feaf7887d023
Revises: 737cae4d1c76, 163471a04985
Create Date: 2020-05-21 14:29:59.577236

"""

# revision identifiers, used by Alembic.
revision = 'feaf7887d023'
down_revision = ('737cae4d1c76', '163471a04985')
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

