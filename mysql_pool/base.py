from sqlalchemy.pool import QueuePool
from sqlalchemy import event

from django.db.backends.mysql.base import *
from functools import partial

import logging


def null(*args, **kwargs):
    pass

mypool = QueuePool(null, **settings.DATABASE_POOL_ARGS)
log = logging.getLogger('z.pool')

def _log(message, *args):
    log.debug('%s to %s' % (message, args[0].get_host_info()))

event.listen(QueuePool, 'checkout', partial(_log, 'retrieved from pool'))
event.listen(QueuePool, 'checkin', partial(_log, 'returned to pool'))
event.listen(QueuePool, 'connect', partial(_log, 'new connection'))

# DATABASE_POOL_ARGS should be something like:
# {'max_overflow':10, 'pool_size':5, 'recycle':300}

class DatabaseWrapper(DatabaseWrapper):

    def _cursor(self):
        if not self._valid_connection():
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

            mypool._creator = partial(Database.connect, **kwargs)
            self.connection = mypool.connect()

            self.connection.encoders[SafeUnicode] = self.connection.encoders[unicode]
            self.connection.encoders[SafeString] = self.connection.encoders[str]
            connection_created.send(sender=self.__class__, connection=self)

        cursor = CursorWrapper(self.connection.cursor())
        return cursor
