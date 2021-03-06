"""replace budget_authority_available_cpe with total_budgetary_resources_cpe in Appropriations table:


Revision ID: 17105e26eef4
Revises: 7e54c1a1c341
Create Date: 2018-04-24 14:09:17.943349

"""

# revision identifiers, used by Alembic.
revision = '17105e26eef4'
down_revision = '7e54c1a1c341'
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
    op.execute("ALTER TABLE appropriation RENAME COLUMN budget_authority_available_cpe TO total_budgetary_resources_cpe")
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("ALTER TABLE appropriation RENAME COLUMN total_budgetary_resources_cpe TO budget_authority_available_cpe")
    # ### end Alembic commands ###

