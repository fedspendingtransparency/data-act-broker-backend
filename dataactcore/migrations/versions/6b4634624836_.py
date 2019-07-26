"""empty message

Revision ID: 6b4634624836
Revises: 4ebc7a781b31, 3244aa7611ef
Create Date: 2019-07-26 23:11:16.768086

"""

# revision identifiers, used by Alembic.
revision = '6b4634624836'
down_revision = ('4ebc7a781b31', '3244aa7611ef')
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

