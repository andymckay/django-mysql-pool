from sqlalchemy.pool import manage, QueuePool
from sqlalchemy import event

from django.conf import settings
from django.utils import importlib
from functools import partial

import hashlib
import logging


log = logging.getLogger('z.pool')


def _log(message, *args):
    log.debug('%s to %s' % (message, args[0].get_host_info()))

# Only hook up the listeners if we are in debug mode.
if settings.DEBUG:
    event.listen(QueuePool, 'checkout', partial(_log, 'retrieved from pool'))
    event.listen(QueuePool, 'checkin', partial(_log, 'returned to pool'))
    event.listen(QueuePool, 'connect', partial(_log, 'new connection'))

# default to the django default db backend, use the setting if defined.
pool_args = getattr(settings, 'DATABASE_POOL_ARGS', {})
db_backend = pool_args.pop('backend', 'django.db.backends.mysql.base')

backend_module = importlib.import_module(db_backend)

# DATABASE_POOL_ARGS should be something like:
# {'max_overflow':10, 'pool_size':5, 'recycle':300}
db_pool = manage(backend_module.Database, **pool_args)


def serialize(**kwargs):
    # We need to figure out what database connection goes where
    # so we'll hash the args.
    keys = sorted(kwargs.keys())
    out = [repr(k) + repr(kwargs[k])
           for k in keys if isinstance(kwargs[k], (str, int, bool))]
    return hashlib.md5(''.join(out)).hexdigest()


class DatabaseCreation(backend_module.DatabaseCreation):
    # The creation flips around between databases in a way that the pool
    # doesn't like. After the db is created, reset the pool.
    def _create_test_db(self, *args):
        result = super(DatabaseCreation, self)._create_test_db(*args)
        db_pool.close()
        return result


class DatabaseWrapper(backend_module.DatabaseWrapper):
    # Unfortunately we have to override the whole cursor function
    # so that Django will pick up our managed Database class.
    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        self.creation = DatabaseCreation(self)

    def _serialize(self, settings_dict=None):
        if settings_dict is None:
            settings_dict = self.settings_dict

        kwargs = {
            'conv': backend_module.django_conversions,
            'charset': 'utf8',
            'use_unicode': True,
        }

        if settings_dict['USER']:
            kwargs['user'] = settings_dict['USER']
        if settings_dict['NAME']:
            kwargs['db'] = settings_dict['NAME']
        if settings_dict['PASSWORD']:
            kwargs['passwd'] = settings_dict['PASSWORD']
        if settings_dict['HOST'].startswith('/'):
            kwargs['unix_socket'] = settings_dict['HOST']
        elif settings_dict['HOST']:
            kwargs['host'] = settings_dict['HOST']
        if settings_dict['PORT']:
            kwargs['port'] = int(settings_dict['PORT'])
        # We need the number of potentially affected rows after an
        # "UPDATE", not the number of changed rows.
        kwargs['client_flag'] = backend_module.CLIENT.FOUND_ROWS
        kwargs.update(settings_dict['OPTIONS'])
        # SQL Alchemy can't serialize the dict that's in OPTIONS, so
        # we'll do some serialization ourselves. You can avoid this
        # step specifying sa_pool_key in the DB settings.
        kwargs['sa_pool_key'] = serialize(**kwargs)
        return kwargs

    def _is_valid_connection(self):
        # If you don't want django to check that the connection is valid,
        # then set DATABASE_POOL_CHECK to False.
        if getattr(settings, 'DATABASE_POOL_CHECK', True):
            return self._valid_connection()
        return False

    def _cursor(self):
        if not self._is_valid_connection():
            _settings = self._serialize()
            self.connection = db_pool.connect(**_settings)

            self.connection.encoders[backend_module.SafeUnicode] =\
                    self.connection.encoders[unicode]
            self.connection.encoders[backend_module.SafeString] =\
                    self.connection.encoders[str]

            backend_module.connection_created.send(sender=self.__class__,
                                                   connection=self)

        cursor = backend_module.CursorWrapper(self.connection.cursor())
        return cursor
