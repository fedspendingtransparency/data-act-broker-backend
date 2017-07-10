"""merging 551a8e1cc551 df2f541291a5

Revision ID: bd821df63bb1
Revises: 551a8e1cc551, df2f541291a5
Create Date: 2017-07-10 12:15:54.108864

"""

# revision identifiers, used by Alembic.
revision = 'bd821df63bb1'
down_revision = ('551a8e1cc551', 'df2f541291a5')
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

