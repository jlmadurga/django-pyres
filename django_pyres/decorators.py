from functools import update_wrapper, wraps
from django_pyres.conf import settings
from django import db
from .core import pyres

try:
    # This is prefered in >1.6
    close_connections = db.close_old_connections
except AttributeError:
    # This is deprecated in 1.6 and removed in 1.8
    close_connections = db.close_connection

def reset_db_connection(func):
    def wrapper(*args, **kwargs):
        """ Because forked processes using connection pool
        can throw errors, close db to reinitialize inside
        child process. """
        if settings.PYRES_USE_QUEUE:
            close_connections()
        result = func(*args, **kwargs)
        if settings.PYRES_USE_QUEUE:
            close_connections()
        return result
    update_wrapper(wrapper, func)
    return wrapper

class Job(object):
    """
Class that wraps a function to enqueue in pyres
"""
    _resque_conn = pyres

    def __init__(self, func, queue):
        self.func = reset_db_connection(func)
        #self.priority = priority

        # Allow this class to be called by pyres
        self.queue = str(queue)
        self.perform = self.__call__

        # Wrap func
        update_wrapper(self, func)

    # _resque wraps the underlying resque connection and delays initialization
    # until needed
    @property
    def _resque(self):
        return self._resque_conn

    @_resque.setter # NOQA
    def _resque(self, val):
        self._resque_conn = val

    def enqueue(self, *args, **kwargs):
        if settings.PYRES_USE_QUEUE:
            queue = kwargs.pop('queue', self.queue)
            if kwargs:
                raise Exception("Cannot pass kwargs to enqueued tasks")
            class_str = '%s.%s' % (self.__module__, self.__name__)
            self._resque.enqueue_from_string(class_str, queue, *args)
        else:
            return self(*args, **kwargs)

    def enqueue_at(self, dt, *args, **kwargs):
        queue = kwargs.pop('queue', self.queue)
        if kwargs:
            raise Exception('Cannot pass kwargs to enqueued tasks')
        class_str = '%s.%s' % (self.__module__, self.__name__)
        self._resque.enqueue_at_from_string(dt, class_str, queue, *args)

    def __call__(self, *args, **kwargs):
        try:
            return self.func(*args, **kwargs)
        except:
            if hasattr(settings, 'RAVEN_CONFIG'):
                from raven.contrib.django.models import client
                client.captureException()
            raise

    def __repr__(self):
        return 'Job(func=%s, queue=%s)' % (self.func, self.queue)


def job(queue, cls=Job):
    def _task(f):
        return cls(f, queue)
    return _task
