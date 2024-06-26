"""Replace quarterly_revalidation_threshold with submission_window_schedule

Revision ID: 471273c020c5
Revises: 9472a2385e99
Create Date: 2020-05-28 11:11:01.594022

"""

# revision identifiers, used by Alembic.
revision = '471273c020c5'
down_revision = '9472a2385e99'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('submission_window_schedule',
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('submission_window_schedule_id', sa.Integer(), nullable=False),
        sa.Column('year', sa.Integer(), nullable=False),
        sa.Column('period', sa.Integer(), nullable=False),
        sa.Column('period_start', sa.DateTime(), nullable=True),
        sa.Column('publish_deadline', sa.DateTime(), nullable=True),
        sa.Column('certification_deadline', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('submission_window_schedule_id')
    )
    op.drop_table('quarterly_revalidation_threshold')
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('quarterly_revalidation_threshold',
        sa.Column('created_at', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
        sa.Column('updated_at', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
        sa.Column('quarterly_revalidation_threshold_id', sa.INTEGER(), nullable=False),
        sa.Column('year', sa.INTEGER(), autoincrement=False, nullable=False),
        sa.Column('quarter', sa.INTEGER(), autoincrement=False, nullable=False),
        sa.Column('window_start', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
        sa.Column('window_end', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
        sa.PrimaryKeyConstraint('quarterly_revalidation_threshold_id', name='quarterly_revalidation_threshold_pkey')
    )
    op.drop_table('submission_window_schedule')
    # ### end Alembic commands ###

