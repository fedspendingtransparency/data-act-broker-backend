"""Merge 4057e3a884d5 e2aef4260e1f

Revision ID: d7476f20b59f
Revises: 4057e3a884d5, e2aef4260e1f
Create Date: 2016-11-18 18:49:57.312588

"""

# revision identifiers, used by Alembic.
revision = 'd7476f20b59f'
down_revision = ('4057e3a884d5', 'e2aef4260e1f')
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

