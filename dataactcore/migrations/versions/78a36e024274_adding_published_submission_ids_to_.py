"""Adding published_submission_ids and certified flag to submission table

Revision ID: 78a36e024274
Revises: 24331fcfcd00
Create Date: 2020-06-04 20:01:06.814406

"""

# revision identifiers, used by Alembic.
revision = '78a36e024274'
down_revision = '24331fcfcd00'
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
    op.add_column('submission', sa.Column('published_submission_ids', sa.ARRAY(sa.Integer()), nullable=True,
                                          server_default="{}"))
    op.add_column('submission', sa.Column('certified', sa.Boolean(), server_default='False', nullable=False))
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('submission', 'published_submission_ids')
    op.drop_column('submission', 'certified')
    # ### end Alembic commands ###

