"""Making cast_as_date exception safe

Revision ID: fbff6e50612e
Revises: 150ac67ac1d3
Create Date: 2026-10-26 21:41:09.036738

"""

# revision identifiers, used by Alembic.
revision = 'fbff6e50612e'
down_revision = '150ac67ac1d3'
branch_labels = None
depends_on = None

from alembic import op


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_data_broker():
    op.execute("""
        CREATE OR REPLACE FUNCTION cast_as_date(TEXT) RETURNS DATE AS $$
        BEGIN
            RETURN CAST($1 AS DATE);
        EXCEPTION WHEN others THEN
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql
        IMMUTABLE
        RETURNS NULL ON NULL INPUT
    """)


def downgrade_data_broker():
    op.execute("""
        CREATE OR REPLACE FUNCTION cast_as_date(TEXT) RETURNS DATE
        AS 'select cast($1 as DATE)'
        LANGUAGE SQL
        IMMUTABLE
        RETURNS NULL ON NULL INPUT
    """)
