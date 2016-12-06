"""Add admin field to User

Revision ID: 963cad0fd72a
Revises: 9f7136e38e21
Create Date: 2016-12-06 19:04:31.674777

"""

# revision identifiers, used by Alembic.
revision = '963cad0fd72a'
down_revision = '9f7136e38e21'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_data_broker():
    op.add_column('users', sa.Column('website_admin', sa.Boolean(), server_default='False', nullable=False))
    op.execute("""
        UPDATE users SET website_admin = True, permission_type_id = 3
        WHERE permission_type_id = 4
    """)


def downgrade_data_broker():
    op.execute("""
        UPDATE users SET permission_type_id = 4
        WHERE website_admin = True
    """)
    op.drop_column('users', 'website_admin')
