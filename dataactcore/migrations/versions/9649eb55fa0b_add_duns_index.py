"""add_duns_index

Revision ID: 9649eb55fa0b
Revises: ff4728a82180
Create Date: 2017-08-07 23:38:29.194844

"""

# revision identifiers, used by Alembic.
revision = '9649eb55fa0b'
down_revision = 'ff4728a82180'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_data_broker():
    op.create_index('ix_duns_awardee_or_recipient_uniqu', 'duns', ['awardee_or_recipient_uniqu'], unique=False)


def downgrade_data_broker():
    op.drop_index('ix_duns_awardee_or_recipient_uniqu', table_name='duns')

