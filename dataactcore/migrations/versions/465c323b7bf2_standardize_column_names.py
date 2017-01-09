"""Standardize column names

Revision ID: 465c323b7bf2
Revises: 32435c7f73b9
Create Date: 2017-01-06 16:01:03.571430

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '465c323b7bf2'
down_revision = '32435c7f73b9'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_data_broker():
    op.alter_column('tas_lookup', 'beginning_period_of_availability',
                    new_column_name='beginning_period_of_availa')
    op.drop_index('ix_tas_lookup_beginning_period_of_availability',
                  table_name='tas_lookup')
    op.create_index(op.f('ix_tas_lookup_beginning_period_of_availa'),
                    'tas_lookup', ['beginning_period_of_availa'], unique=False)

    op.alter_column('tas_lookup', 'ending_period_of_availability',
                    new_column_name='ending_period_of_availabil')
    op.drop_index('ix_tas_lookup_ending_period_of_availability',
                  table_name='tas_lookup')
    op.create_index(op.f('ix_tas_lookup_ending_period_of_availabil'),
                    'tas_lookup', ['ending_period_of_availabil'], unique=False)


def downgrade_data_broker():
    op.alter_column('tas_lookup', 'beginning_period_of_availa',
                    new_column_name='beginning_period_of_availability')
    op.drop_index('ix_tas_lookup_beginning_period_of_availa',
                  table_name='tas_lookup')
    op.create_index(op.f('ix_tas_lookup_beginning_period_of_availability'),
                    'tas_lookup', ['beginning_period_of_availability'],
                    unique=False)

    op.alter_column('tas_lookup', 'ending_period_of_availabil',
                    new_column_name='ending_period_of_availability')
    op.drop_index('ix_tas_lookup_ending_period_of_availabil',
                  table_name='tas_lookup')
    op.create_index(op.f('ix_tas_lookup_ending_period_of_availability'),
                    'tas_lookup', ['ending_period_of_availability'],
                    unique=False)
