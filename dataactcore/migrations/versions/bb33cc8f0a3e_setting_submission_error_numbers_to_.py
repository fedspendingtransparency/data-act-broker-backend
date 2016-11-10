"""setting submission error numbers to default zero

Revision ID: bb33cc8f0a3e
Revises: a97dabbd44f4
Create Date: 2016-11-10 11:08:55.746848

"""

# revision identifiers, used by Alembic.
revision = 'bb33cc8f0a3e'
down_revision = 'a97dabbd44f4'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_data_broker():
    op.execute('UPDATE job SET number_of_errors = 0 WHERE number_of_errors IS NULL')
    op.alter_column('job', 'number_of_errors',
               existing_type=sa.INTEGER(),
               nullable=False)
    op.execute('UPDATE job SET number_of_warnings = 0 WHERE number_of_warnings IS NULL')
    op.alter_column('job', 'number_of_warnings',
               existing_type=sa.INTEGER(),
               nullable=False)
    op.execute('UPDATE submission SET number_of_errors = 0 WHERE number_of_errors IS NULL')
    op.alter_column('submission', 'number_of_errors',
               existing_type=sa.INTEGER(),
               nullable=False)
    op.execute('UPDATE submission SET number_of_warnings = 0 WHERE number_of_warnings IS NULL')
    op.alter_column('submission', 'number_of_warnings',
               existing_type=sa.INTEGER(),
               nullable=False)


def downgrade_data_broker():
    op.alter_column('submission', 'number_of_warnings',
               existing_type=sa.INTEGER(),
               nullable=True)
    op.alter_column('submission', 'number_of_errors',
               existing_type=sa.INTEGER(),
               nullable=True)
    op.alter_column('job', 'number_of_warnings',
               existing_type=sa.INTEGER(),
               nullable=True)
    op.alter_column('job', 'number_of_errors',
               existing_type=sa.INTEGER(),
               nullable=True)

