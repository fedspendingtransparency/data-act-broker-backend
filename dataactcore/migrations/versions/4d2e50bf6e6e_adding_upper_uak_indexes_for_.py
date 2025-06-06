""" Adding upper uak indexes for transaction tables

Revision ID: 4d2e50bf6e6e
Revises: e632d7e0bcb5
Create Date: 2025-03-04 00:23:20.282581

"""

# revision identifiers, used by Alembic.
revision = '4d2e50bf6e6e'
down_revision = 'e632d7e0bcb5'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_index('ix_dap_uak_upper', 'detached_award_procurement', [text('UPPER(unique_award_key)')], unique=False)
    op.create_index('ix_published_fabs_uak_upper', 'published_fabs', [text('UPPER(unique_award_key)')], unique=False)
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('ix_published_fabs_uak_upper', table_name='published_fabs')
    op.drop_index('ix_dap_uak_upper', table_name='detached_award_procurement')
    # ### end Alembic commands ###

