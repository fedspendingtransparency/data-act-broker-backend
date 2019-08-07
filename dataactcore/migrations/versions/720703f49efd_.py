"""empty message

Revision ID: 720703f49efd
Revises: 3244aa7611ef, 87d7a9b0ea7b
Create Date: 2019-08-07 13:52:18.842336

"""

# revision identifiers, used by Alembic.
revision = '720703f49efd'
down_revision = ('3244aa7611ef', '87d7a9b0ea7b')
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

