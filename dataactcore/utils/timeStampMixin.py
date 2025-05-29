from datetime import datetime, UTC

from sqlalchemy import Column, DateTime, event


class TimeStampMixin(object):
    """Timestamping mixin"""

    created_at = Column(DateTime, default=lambda: datetime.now(UTC).replace(tzinfo=None))
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC).replace(tzinfo=None))

    @staticmethod
    def _updated_at(mapper, connection, target):
        if not getattr(target, "ignore_updated_at", None):
            target.updated_at = datetime.now(UTC).replace(tzinfo=None)

    @classmethod
    def __declare_last__(cls):
        event.listen(cls, "before_update", cls._updated_at)


class TimeStampBase(TimeStampMixin):
    pass
