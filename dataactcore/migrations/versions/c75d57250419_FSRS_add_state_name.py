"""Adding legal entity, place of performance state name columns to all FSRS tables

Revision ID: c75d57250419
Revises: 3ff0ad501645
Create Date: 2018-03-22 12:52:12.495263

"""

# revision identifiers, used by Alembic.
revision = 'c75d57250419'
down_revision = '3ff0ad501645'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_data_broker():
    op.add_column('fsrs_grant', sa.Column('awardee_address_state_name', sa.String(), nullable=True))
    op.add_column('fsrs_grant', sa.Column('principle_place_state_name', sa.String(), nullable=True))
    op.add_column('fsrs_procurement', sa.Column('company_address_state_name', sa.String(), nullable=True))
    op.add_column('fsrs_procurement', sa.Column('principle_place_state_name', sa.String(), nullable=True))
    op.add_column('fsrs_subcontract', sa.Column('company_address_state_name', sa.String(), nullable=True))
    op.add_column('fsrs_subcontract', sa.Column('principle_place_state_name', sa.String(), nullable=True))
    op.add_column('fsrs_subgrant', sa.Column('awardee_address_state_name', sa.String(), nullable=True))
    op.add_column('fsrs_subgrant', sa.Column('principle_place_state_name', sa.String(), nullable=True))
    ### end Alembic commands ###


def downgrade_data_broker():
    op.drop_column('fsrs_subgrant', 'principle_place_state_name')
    op.drop_column('fsrs_subgrant', 'awardee_address_state_name')
    op.drop_column('fsrs_subcontract', 'principle_place_state_name')
    op.drop_column('fsrs_subcontract', 'company_address_state_name')
    op.drop_column('fsrs_procurement', 'principle_place_state_name')
    op.drop_column('fsrs_procurement', 'company_address_state_name')
    op.drop_column('fsrs_grant', 'principle_place_state_name')
    op.drop_column('fsrs_grant', 'awardee_address_state_name')
    ### end Alembic commands ###

