"""update-sf133-datatypes

Revision ID: 5a9051f9bfc5
Revises: a0a4f1ef56ae
Create Date: 2016-08-11 11:44:49.640398

"""

# revision identifiers, used by Alembic.
revision = '5a9051f9bfc5'
down_revision = 'a0a4f1ef56ae'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_data_broker():
    op.alter_column('sf_133', 'fiscal_year', nullable=False)
    op.execute('ALTER TABLE sf_133 ALTER COLUMN fiscal_year TYPE INTEGER USING (fiscal_year::integer)')

    op.alter_column('sf_133', 'period', nullable=False)
    op.execute('ALTER TABLE sf_133 ALTER COLUMN period TYPE INTEGER USING (period::integer)')


def downgrade_data_broker():
    op.alter_column('sf_133', 'period',
               existing_type=sa.Integer(),
               type_=sa.TEXT(),
               nullable=True)
    op.alter_column('sf_133', 'fiscal_year',
               existing_type=sa.Integer(),
               type_=sa.TEXT(),
               nullable=True)

