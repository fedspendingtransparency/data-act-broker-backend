"""empty message

Revision ID: 3356595f395f
Revises: 08c4db2f3007, 591024d9958f
Create Date: 2019-03-06 14:31:04.827405

"""

# revision identifiers, used by Alembic.
revision = '3356595f395f'
down_revision = ('08c4db2f3007', '591024d9958f')
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

