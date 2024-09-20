""" Base metadata for all models to be added to """
from sqlalchemy.orm import declarative_base
from dataactcore.utils.timeStampMixin import TimeStampBase

Base = declarative_base(cls=TimeStampBase)
