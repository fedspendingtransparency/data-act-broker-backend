"""Adding entity_data_source to detached_award_procurement table

Revision ID: 589fcd5224fe
Revises: a1c4e0eb6c26
Create Date: 2022-03-11 11:21:42.673254

"""

# revision identifiers, used by Alembic.
revision = '589fcd5224fe'
down_revision = 'a1c4e0eb6c26'
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
    op.add_column('detached_award_procurement', sa.Column('entity_data_source', sa.Text(), nullable=True))
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('detached_award_procurement', 'entity_data_source')
    # ### end Alembic commands ###

