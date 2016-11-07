"""drop-filetype-validation-table

Revision ID: a97dabbd44f4
Revises: c3a3389bda57
Create Date: 2016-11-07 09:45:25.972784

"""

# revision identifiers, used by Alembic.
revision = 'a97dabbd44f4'
down_revision = 'c3a3389bda57'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from dataactcore.models.baseModel import Base
from dataactcore.models.lookups import FILE_TYPE

Session = sessionmaker()

class FileTypeValidation(Base):
    __tablename__ = 'file_type_validation'

    file_id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.Text)
    description = sa.Column(sa.Text)
    file_order = sa.Column(sa.Integer, nullable=False, server_default="0")

def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()

def upgrade_data_broker():
    # drop the file_type_validation table
    op.drop_table('file_type_validation')

def downgrade_data_broker():
    # recreate and repopulate the file_type_validation table
    bind = op.get_bind()
    session = Session(bind=bind)

    FileTypeValidation.__table__.create(bind)

    for ft in FILE_TYPE:
        fileType = FileTypeValidation(
            file_id=ft.id,
            name=ft.name,
            description=ft.desc,
            file_order=ft.order
        )
        session.merge(fileType)
    session.commit()
