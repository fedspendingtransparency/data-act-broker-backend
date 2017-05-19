"""Add agency name and sub-tier agency name to published award financial assistance

Revision ID: 5664b0e3e179
Revises: ce1087583081
Create Date: 2017-05-19 02:47:18.081619

"""

# revision identifiers, used by Alembic.
revision = '5664b0e3e179'
down_revision = 'ce1087583081'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_data_broker():
    op.add_column('published_award_financial_assistance', sa.Column('awarding_agency_name', sa.Text(), nullable=True))
    op.add_column('published_award_financial_assistance', sa.Column('awarding_sub_tier_agency_n', sa.Text(), nullable=True))
    op.add_column('published_award_financial_assistance', sa.Column('funding_agency_name', sa.Text(), nullable=True))
    op.add_column('published_award_financial_assistance', sa.Column('funding_sub_tier_agency_na', sa.Text(), nullable=True))
    ### end Alembic commands ###


def downgrade_data_broker():
    op.drop_column('published_award_financial_assistance', 'funding_sub_tier_agency_na')
    op.drop_column('published_award_financial_assistance', 'funding_agency_name')
    op.drop_column('published_award_financial_assistance', 'awarding_sub_tier_agency_n')
    op.drop_column('published_award_financial_assistance', 'awarding_agency_name')
    ### end Alembic commands ###

