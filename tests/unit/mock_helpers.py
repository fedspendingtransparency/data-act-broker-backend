class DynamicObject:
    pass


class MockFilter(object):  # This is the ONE parameter constructor
    def __init__(self):
        self._count = 0
        self._first = DynamicObject()

    def first(self):  # This is the another method that's just coming along for the ride.
        return self._first

    def count(self):  # This is the needed Count method
        return self._count


class MockQuery(object):  # This is the ONE parameter constructor
    def __init__(self):
        self._filter = MockFilter()
        self._filter_by = MockFilter()

    def filter(self, place_holder):  # This is used to mimic the query.filter() call
        return self._filter

    def filter_by(self, **kwargs):  # This is used to mimic the query.filter_by() call
        return self._filter_by


class MockSession(object):
    def __init__(self):
        self._query = MockQuery()
        self.dirty = []

    def flush(self):
        pass

    def query(self, place_holder):  # This is used to mimic the session.query call
        return self._query
