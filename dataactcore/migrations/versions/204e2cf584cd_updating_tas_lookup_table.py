"""Add FR Entity Type and FR Entity Description to TAS Lookup table

Revision ID: 204e2cf584cd
Revises: 821a7f4694f0
Create Date: 2017-06-05 03:29:17.584501

"""

# revision identifiers, used by Alembic.
revision = '204e2cf584cd'
down_revision = '821a7f4694f0'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_data_broker():
    op.add_column('tas_lookup', sa.Column('fr_entity_description', sa.Text(), nullable=True))
    op.add_column('tas_lookup', sa.Column('fr_entity_type', sa.Text(), nullable=True))
    ### end Alembic commands ###


def downgrade_data_broker():
    op.drop_column('tas_lookup', 'fr_entity_type')
    op.drop_column('tas_lookup', 'fr_entity_description')
    ### end Alembic commands ###

