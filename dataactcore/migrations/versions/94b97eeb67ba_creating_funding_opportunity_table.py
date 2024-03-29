"""Creating funding_opportunity table

Revision ID: 94b97eeb67ba
Revises: 7212b234d8c5
Create Date: 2023-09-05 12:51:16.108742

"""

# revision identifiers, used by Alembic.
revision = '94b97eeb67ba'
down_revision = '7212b234d8c5'
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
    op.create_table('funding_opportunity',
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('funding_opportunity_id', sa.Integer(), nullable=False),
        sa.Column('funding_opportunity_number', sa.Text(), nullable=False),
        sa.Column('title', sa.Text(), nullable=True),
        sa.Column('cfda_numbers', sa.ARRAY(sa.Text()), nullable=True),
        sa.Column('agency_name', sa.Text(), nullable=True),
        sa.Column('status', sa.Text(), nullable=True),
        sa.Column('open_date', sa.Date(), nullable=True),
        sa.Column('close_date', sa.Date(), nullable=True),
        sa.Column('doc_type', sa.Text(), nullable=True),
        sa.Column('internal_id', sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint('funding_opportunity_id')
    )
    op.create_index(op.f('ix_funding_opportunity_funding_opportunity_number'), 'funding_opportunity', ['funding_opportunity_number'])
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_funding_opportunity_funding_opportunity_number'), table_name='funding_opportunity')
    op.drop_table('funding_opportunity')
    # ### end Alembic commands ###

