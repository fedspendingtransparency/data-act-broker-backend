"""Adding index to file B tas

Revision ID: e7af56680794
Revises: 1b55916099f4
Create Date: 2024-01-19 14:05:43.845848

"""

# revision identifiers, used by Alembic.
revision = 'e7af56680794'
down_revision = '1b55916099f4'
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
    op.create_index(op.f('ix_object_class_program_activity_tas'), 'object_class_program_activity', ['tas'], unique=False)
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_object_class_program_activity_tas'), table_name='object_class_program_activity')
    # ### end Alembic commands ###

