from threading import Thread
import functools

def timeout(timeout, message=None):
    def deco(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            defaultMessage = 'function [%s] timeout [%s seconds] exceeded!' % (func.__name__, timeout)
            res = [Exception(message or defaultMessage)]
            def newFunc():
                try:
                    res[0] = func(*args, **kwargs)
                except Exception, e:
                    res[0] = e
            t = Thread(target=newFunc)
            t.daemon = True
            try:
                t.start()
                t.join(timeout)
            except Exception, je:
                print 'error starting thread'
                raise je
            ret = res[0]
            if isinstance(ret, BaseException):
                raise ret
            return ret
        return wrapper
    return deco