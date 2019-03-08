"""file f functions

Revision ID: ba7e8e488c36
Revises: 3356595f395f
Create Date: 2019-03-08 14:24:23.878278

"""

# revision identifiers, used by Alembic.
revision = 'ba7e8e488c36'
down_revision = '3356595f395f'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_data_broker():
    op.execute("""
        CREATE OR REPLACE function cfda_num(TEXT) returns TEXT AS $$
        BEGIN
            $1 = TRIM($1);
            RETURN TRIM(LEFT($1, POSITION(' ' IN $1)));
        EXCEPTION WHEN others THEN
            return NULL;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE OR REPLACE function cfda_word(TEXT) returns TEXT AS $$
        BEGIN
            $1 = TRIM($1);
            RETURN TRIM(RIGHT($1, LENGTH($1)-POSITION(' ' IN $1)));
        EXCEPTION WHEN others THEN
            return NULL;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE OR REPLACE function cfda_num_loop(TEXT) returns TEXT AS $$
        DECLARE
            s TEXT;
            i TEXT;
        BEGIN
            FOREACH i IN ARRAY regexp_split_to_array($1, ';')
            LOOP
                s := CONCAT(s, ', ', cfda_num(i));
            END LOOP;
            RETURN RIGHT(s, -2);
        EXCEPTION WHEN others THEN
            return NULL;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE OR REPLACE function cfda_word_loop(TEXT) RETURNS TEXT AS $$
        DECLARE
            s TEXT;
            i TEXT;
        BEGIN
            FOREACH i IN ARRAY regexp_split_to_array($1, ';')
            LOOP
                s := CONCAT(s, ', ', cfda_word(i));
            END LOOP;
            RETURN RIGHT(s, -2);
        EXCEPTION WHEN others THEN
            return NULL;
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute("""
        CREATE OR REPLACE FUNCTION fy(raw_date DATE) RETURNS integer AS $$
          DECLARE result INTEGER;
          DECLARE month_num INTEGER;
          BEGIN 
            month_num := EXTRACT(MONTH from raw_date);
            result := EXTRACT(YEAR FROM raw_date);
            IF month_num > 9    
            THEN
              result := result + 1;
            END IF;
            RETURN result;
        END;
        $$ LANGUAGE plpgsql;
    """)
    pass


def downgrade_data_broker():
    op.execute(""" DROP FUNCTION IF EXISTS cfda_num(TEXT) """)
    op.execute(""" DROP FUNCTION IF EXISTS cfda_word(TEXT) """)
    op.execute(""" DROP FUNCTION IF EXISTS cfda_num_loop(TEXT) """)
    op.execute(""" DROP FUNCTION IF EXISTS cfda_word_loop(TEXT) """)
    op.execute(""" DROP FUNCTION IF EXISTS fy(DATE) """)
    pass

