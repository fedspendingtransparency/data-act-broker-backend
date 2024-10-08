"""Adding DEFC columns

Revision ID: 7cb410fcdc60
Revises: 9a5e71474232
Create Date: 2024-02-15 17:19:50.018546

"""

# revision identifiers, used by Alembic.
revision = '7cb410fcdc60'
down_revision = '9a5e71474232'
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
    op.add_column('defc', sa.Column('public_laws', sa.ARRAY(sa.Text()), nullable=True))
    op.add_column('defc', sa.Column('public_law_short_titles', sa.ARRAY(sa.Text()), nullable=True))
    op.add_column('defc', sa.Column('urls', sa.ARRAY(sa.Text()), nullable=True))
    op.add_column('defc', sa.Column('is_valid', sa.Boolean(), server_default='True', nullable=False))
    op.add_column('defc', sa.Column('earliest_pl_action_date', sa.DateTime(), nullable=True))
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('defc', 'earliest_pl_action_date')
    op.drop_column('defc', 'is_valid')
    op.drop_column('defc', 'urls')
    op.drop_column('defc', 'public_law_short_titles')
    op.drop_column('defc', 'public_laws')
    # ### end Alembic commands ###

