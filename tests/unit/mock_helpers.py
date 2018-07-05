from unittest.mock import Mock


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


def mock_response(
        status=200,
        content="CONTENT",
        json_data=None,
        raise_for_status=None,
        url=None):
    """
    since we typically test a bunch of different
    requests calls for a service, we are going to do
    a lot of mock responses, so its usually a good idea
    to have a helper function that builds these things
    """
    mock_resp = Mock()
    # mock raise_for_status call w/optional error
    mock_resp.raise_for_status = Mock()
    if raise_for_status:
        mock_resp.raise_for_status.side_effect = raise_for_status
    # set status code and content
    mock_resp.status_code = status
    mock_resp.content = content
    mock_resp.url = url
    # add json data if provided
    if json_data:
        mock_resp.json = Mock(
            return_value=json_data
        )
    return mock_resp
