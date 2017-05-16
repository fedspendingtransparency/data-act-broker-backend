"""Add cfda program table

Revision ID: 844f2d3a614a
Revises: f15485f0092b
Create Date: 2017-05-15 17:04:50.407153

"""

# revision identifiers, used by Alembic.
revision = '844f2d3a614a'
down_revision = 'f15485f0092b'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()


def upgrade_data_broker():
    op.create_table('cfda_program',
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.Column('cfda_program_id', sa.Integer(), nullable=False),
    sa.Column('program_number', sa.Float(), nullable=False),
    sa.Column('program_title', sa.Text(), nullable=True),
    sa.Column('popular_name', sa.Text(), nullable=True),
    sa.Column('federal_agency', sa.Text(), nullable=True),
    sa.Column('authorization', sa.Text(), nullable=True),
    sa.Column('objectives', sa.Text(), nullable=True),
    sa.Column('types_of_assistance', sa.Text(), nullable=True),
    sa.Column('uses_and_use_restrictions', sa.Text(), nullable=True),
    sa.Column('applicant_eligibility', sa.Text(), nullable=True),
    sa.Column('beneficiary_eligibility', sa.Text(), nullable=True),
    sa.Column('credentials_documentation', sa.Text(), nullable=True),
    sa.Column('preapplication_coordination', sa.Text(), nullable=True),
    sa.Column('application_procedures', sa.Text(), nullable=True),
    sa.Column('award_procedure', sa.Text(), nullable=True),
    sa.Column('deadlines', sa.Text(), nullable=True),
    sa.Column('range_of_approval_disapproval_time', sa.Text(), nullable=True),
    sa.Column('website_address', sa.Text(), nullable=True),
    sa.Column('formula_and_matching_requirements', sa.Text(), nullable=True),
    sa.Column('length_and_time_phasing_of_assistance', sa.Text(), nullable=True),
    sa.Column('reports', sa.Text(), nullable=True),
    sa.Column('audits', sa.Text(), nullable=True),
    sa.Column('records', sa.Text(), nullable=True),
    sa.Column('account_identification', sa.Text(), nullable=True),
    sa.Column('obligations', sa.Text(), nullable=True),
    sa.Column('range_and_average_of_financial_assistance', sa.Text(), nullable=True),
    sa.Column('appeals', sa.Text(), nullable=True),
    sa.Column('renewals', sa.Text(), nullable=True),
    sa.Column('program_accomplishments', sa.Text(), nullable=True),
    sa.Column('regulations_guidelines_and_literature', sa.Text(), nullable=True),
    sa.Column('regional_or_local_office', sa.Text(), nullable=True),
    sa.Column('headquarters_office', sa.Text(), nullable=True),
    sa.Column('related_programs', sa.Text(), nullable=True),
    sa.Column('examples_of_funded_projects', sa.Text(), nullable=True),
    sa.Column('criteria_for_selecting_proposals', sa.Text(), nullable=True),
    sa.Column('url', sa.Text(), nullable=True),
    sa.Column('recovery', sa.Text(), nullable=True),
    sa.Column('omb_agency_code', sa.Text(), nullable=True),
    sa.Column('omb_bureau_code', sa.Text(), nullable=True),
    sa.Column('published_date', sa.Text(), nullable=True),
    sa.Column('archived_date', sa.Text(), nullable=True),
    sa.PrimaryKeyConstraint('cfda_program_id')
    )
    ### end Alembic commands ###


def downgrade_data_broker():
    op.drop_table('cfda_program')
    ### end Alembic commands ###

