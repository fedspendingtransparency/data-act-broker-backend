"""Add cfda title to published

Revision ID: 5576159ed137
Revises: f15485f0092b
Create Date: 2017-05-14 21:30:24.007049

"""

# revision identifiers, used by Alembic.
revision = '5576159ed137'
down_revision = 'f15485f0092b'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_data_broker():
    op.add_column('detached_award_financial_assistance', sa.Column('cfda_title', sa.Text(), nullable=True))
    op.add_column('published_award_financial_assistance', sa.Column('cfda_title', sa.Text(), nullable=True))
    ### end Alembic commands ###


def downgrade_data_broker():
    op.drop_column('published_award_financial_assistance', 'cfda_title')
    op.drop_column('detached_award_financial_assistance', 'cfda_title')
    ### end Alembic commands ###

