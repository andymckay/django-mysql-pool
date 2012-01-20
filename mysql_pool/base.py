from sqlalchemy.pool import manage, QueuePool
from sqlalchemy import event

from django.db.backends.mysql.base import *
from functools import partial

import hashlib
import logging


log = logging.getLogger('z.pool')

def _log(message, *args):
    log.debug('%s to %s' % (message, args[0].get_host_info()))

event.listen(QueuePool, 'checkout', partial(_log, 'retrieved from pool'))
event.listen(QueuePool, 'checkin', partial(_log, 'returned to pool'))
event.listen(QueuePool, 'connect', partial(_log, 'new connection'))

# DATABASE_POOL_ARGS should be something like:
# {'max_overflow':10, 'pool_size':5, 'recycle':300}

Database = manage(Database, **getattr(settings, 'DATABASE_POOL_ARGS', {}))


def serialize(**kwargs):
    # We need to figure out what database connection goes where
    # so we'll hash the args.
    return hashlib.md5(str(kwargs)).hexdigest()


class DatabaseWrapper(DatabaseWrapper):
    # Unfortunately we have to override the whole cursor function
    # so that Django will pick up our managed Database class.

    def _set_settings(self):
        kwargs = {
            'conv': django_conversions,
            'charset': 'utf8',
            'use_unicode': True,
        }

        settings_dict = self.settings_dict
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
        kwargs['client_flag'] = CLIENT.FOUND_ROWS
        kwargs.update(settings_dict['OPTIONS'])
        # SQL Alchemy can't serialize the dict that's in OPTIONS, so
        # we'll do some serialization ourselves. You can avoid this
        # step specifying sa_pool_key in the DB settings.
        if 'sa_pool_key' not in kwargs:
            kwargs['sa_pool_key'] = serialize(**kwargs)
        self._settings_dict = kwargs

    def _cursor(self):
        if not getattr(self, '_settings_dict', None):
            self._set_settings()

        self.connection = Database.connect(**self._settings_dict)
        self.connection.encoders[SafeUnicode] = self.connection.encoders[unicode]
        self.connection.encoders[SafeString] = self.connection.encoders[str]
        connection_created.send(sender=self.__class__, connection=self)
        cursor = CursorWrapper(self.connection.cursor())
        return cursor
