"""Adding column legal entity name to Executive Compensation

Revision ID: ee7bff1d660c
Revises: c75d57250419
Create Date: 2018-03-22 12:55:32.951821

"""

# revision identifiers, used by Alembic.
revision = 'ee7bff1d660c'
down_revision = 'c75d57250419'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_data_broker():
    op.add_column('executive_compensation', sa.Column('awardee_or_recipient_legal', sa.Text(), nullable=True))


def downgrade_data_broker():
    op.drop_column('executive_compensation', 'awardee_or_recipient_legal')

