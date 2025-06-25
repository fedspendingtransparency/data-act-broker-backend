from threading import Thread
import functools


def timeout(timeout_length, message=None):
    """Creates timeout decorator to be attached to functions"""

    def deco(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            default_message = "function [%s] timeout [%s seconds] exceeded!" % (func.__name__, timeout_length)
            res = [Exception(message or default_message)]

            def new_func():
                try:
                    res[0] = func(*args, **kwargs)
                except Exception as e:
                    res[0] = e

            t = Thread(target=new_func)
            t.daemon = True
            try:
                t.start()
                t.join(timeout_length)
            except Exception as je:
                print("error starting thread")
                raise je
            ret = res[0]
            if isinstance(ret, BaseException):
                raise ret
            return ret

        return wrapper

    return deco
