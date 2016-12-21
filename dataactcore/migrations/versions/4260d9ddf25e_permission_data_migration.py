"""Permission data migration

Revision ID: 4260d9ddf25e
Revises: 62c18e9012fd
Create Date: 2016-12-07 20:34:43.256700

"""

# revision identifiers, used by Alembic.
revision = '4260d9ddf25e'
down_revision = '62c18e9012fd'
branch_labels = None
depends_on = None

from alembic import op


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_data_broker():
    op.execute("""
        INSERT INTO user_affiliation (user_id, cgac_id, permission_type_id)
        SELECT users.user_id, cgac.cgac_id, users.permission_type_id
        FROM users INNER JOIN cgac ON (cgac.cgac_code = users.cgac_code)
        WHERE users.permission_type_id IS NOT NULL
    """)


def downgrade_data_broker():
    op.execute("TRUNCATE user_affiliation")
