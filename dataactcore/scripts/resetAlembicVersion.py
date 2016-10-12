import argparse
from dataactcore.models.errorInterface import ErrorInterface
from dataactcore.models.jobTrackerInterface import JobTrackerInterface
from dataactcore.models.userInterface import UserInterface
from dataactcore.models.validationInterface import ValidationInterface
from sqlalchemy import MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import update


def reset_alembic(alembic_version):

    engine_list = [
        ErrorInterface().engine,
        JobTrackerInterface().engine,
        UserInterface().engine,
        ValidationInterface().engine,
    ]

    for e in engine_list:
        Session = sessionmaker(bind=e)
        session = Session()
        metadata = MetaData(bind=e)
        alembic_table = Table('alembic_version', metadata, autoload=True)
        u = update(alembic_table)
        u = u.values({"version_num": alembic_version})
        session.execute(u)
        session.commit()

parser = argparse.ArgumentParser\
    (description="Reset alembic version tables across broker databases.")
parser.add_argument(
    'version', help="Version to set the Alembic migration tables to.")
v = vars(parser.parse_args())['version']
reset_alembic(v)

