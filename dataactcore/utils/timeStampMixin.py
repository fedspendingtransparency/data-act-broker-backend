from datetime import datetime
from sqlalchemy import Column, DateTime, event


class TimeStampMixin(object):
    """ Timestamping mixin
    """
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

    @staticmethod
    def _updated_at(mapper, connection, target):
        if not getattr(target, "ignore_updated_at", None):
            target.updated_at = datetime.utcnow()

    @classmethod
    def __declare_last__(cls):
        event.listen(cls, 'before_update', cls._updated_at)


class TimeStampBase(TimeStampMixin):
    pass
