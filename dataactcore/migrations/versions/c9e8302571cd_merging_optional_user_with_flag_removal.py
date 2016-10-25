"""merging_optional_user_with_flag_removal

Revision ID: c9e8302571cd
Revises: 159caff749a3, ea8fbaa044d7
Create Date: 2016-10-19 14:40:05.237033

"""

# revision identifiers, used by Alembic.
revision = 'c9e8302571cd'
down_revision = ('159caff749a3', 'ea8fbaa044d7')
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

