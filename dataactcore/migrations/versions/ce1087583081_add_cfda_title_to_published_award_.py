"""Add cfda title to published award financial assistance

Revision ID: ce1087583081
Revises: 844f2d3a614a
Create Date: 2017-05-16 04:26:33.175983

"""

# revision identifiers, used by Alembic.
revision = 'ce1087583081'
down_revision = '844f2d3a614a'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()



def upgrade_data_broker():
    op.add_column('published_award_financial_assistance', sa.Column('cfda_title', sa.Text(), nullable=True))
    ### end Alembic commands ###


def downgrade_data_broker():
    op.drop_column('published_award_financial_assistance', 'cfda_title')
    ### end Alembic commands ###

