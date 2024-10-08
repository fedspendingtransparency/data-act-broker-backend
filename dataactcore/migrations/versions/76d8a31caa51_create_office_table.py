"""Create office table

Revision ID: 76d8a31caa51
Revises: 67feaf4d50b8
Create Date: 2018-09-10 10:55:31.010188

"""

# revision identifiers, used by Alembic.
revision = '76d8a31caa51'
down_revision = '67feaf4d50b8'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('office',
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.Column('office_id', sa.Integer(), nullable=False),
    sa.Column('office_code', sa.Text(), nullable=False),
    sa.Column('office_name', sa.Text(), nullable=True),
    sa.Column('sub_tier_code', sa.Text(), nullable=False),
    sa.Column('agency_code', sa.Text(), nullable=False),
    sa.Column('contracting_office', sa.Boolean(), server_default='False', nullable=False),
    sa.Column('funding_office', sa.Boolean(), server_default='False', nullable=False),
    sa.Column('grant_office', sa.Boolean(), server_default='False', nullable=False),
    sa.PrimaryKeyConstraint('office_id')
    )
    op.create_index(op.f('ix_office_agency_code'), 'office', ['agency_code'], unique=False)
    op.create_index(op.f('ix_office_office_code'), 'office', ['office_code'], unique=True)
    op.create_index(op.f('ix_office_sub_tier_code'), 'office', ['sub_tier_code'], unique=False)
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_office_sub_tier_code'), table_name='office')
    op.drop_index(op.f('ix_office_office_code'), table_name='office')
    op.drop_index(op.f('ix_office_agency_code'), table_name='office')
    op.drop_table('office')
    # ### end Alembic commands ###

