"""Add FREC table lookup

Revision ID: 20ee975316b9
Revises: 204e2cf584cd
Create Date: 2017-06-12 04:38:06.713316

"""

# revision identifiers, used by Alembic.
revision = '20ee975316b9'
down_revision = '204e2cf584cd'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_data_broker():
    op.create_table('frec',
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.Column('frec_id', sa.Integer(), nullable=False),
    sa.Column('frec_code', sa.Text(), nullable=True),
    sa.Column('agency_name', sa.Text(), nullable=True),
    sa.PrimaryKeyConstraint('frec_id')
    )
    ### end Alembic commands ###


def downgrade_data_broker():
    op.drop_index(op.f('ix_frec_frec_code'), table_name='frec')
    op.drop_table('frec')
    ### end Alembic commands ###

