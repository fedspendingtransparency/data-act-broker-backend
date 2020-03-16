"""Add display_tas to certified tables

Revision ID: ce018801e0fa
Revises: 8b22879952cf
Create Date: 2020-02-20 09:26:23.017749

"""

# revision identifiers, used by Alembic.
revision = 'ce018801e0fa'
down_revision = '8b22879952cf'
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
    op.add_column('certified_appropriation', sa.Column('display_tas', sa.Text(), nullable=True))
    op.add_column('certified_award_financial', sa.Column('display_tas', sa.Text(), nullable=True))
    op.add_column('certified_object_class_program_activity', sa.Column('display_tas', sa.Text(), nullable=True))
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('certified_object_class_program_activity', 'display_tas')
    op.drop_column('certified_award_financial', 'display_tas')
    op.drop_column('certified_appropriation', 'display_tas')
    # ### end Alembic commands ###
