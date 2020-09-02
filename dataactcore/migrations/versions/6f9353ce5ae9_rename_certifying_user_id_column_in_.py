"""Rename certifying_user_id column in submission to publishing_user_id

Revision ID: 6f9353ce5ae9
Revises: c5449ab3577f
Create Date: 2020-08-20 10:00:39.260389

"""

# revision identifiers, used by Alembic.
revision = '6f9353ce5ae9'
down_revision = 'c5449ab3577f'
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
    op.drop_constraint('fk_submission_certifying_user', 'submission', type_='foreignkey')
    op.alter_column('submission', 'certifying_user_id', new_column_name='publishing_user_id')
    op.create_foreign_key('fk_submission_publishing_user', 'submission', 'users', ['publishing_user_id'], ['user_id'], ondelete='SET NULL')
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('fk_submission_publishing_user', 'submission', type_='foreignkey')
    op.alter_column('submission', 'publishing_user_id', new_column_name='certifying_user_id')
    op.create_foreign_key('fk_submission_certifying_user', 'submission', 'users', ['certifying_user_id'], ['user_id'], ondelete='SET NULL')
    # ### end Alembic commands ###
