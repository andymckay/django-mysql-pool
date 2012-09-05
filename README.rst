Alter `ENGINE` to `mysql_pool`.
Add some `DATABASE_POOL_ARGS` for example::

    DATABASE_POOL_ARGS = {
        'max_overflow': 10,
        'pool_size':5,
        'recycle':300
    }

Optionally add a `sa_pool_key` value so we don't need to hash each db.

Example::

    'two': {
        'ENGINE': 'mysql_pool',
        'sa_pool_key': 'two',
        ...
    }

Also optionally, you can add a 'backend' key with the importable path to
the backend you want to use. ::

    'backend': 'mysql_pymysql.base'

Currently, we only support mysql_pymysql and the default django mysql backend.

Require SQL Alchemy 0.7 for the events.


TODO:

- some proper tests

- make test utils work with multiple databases, by doing features.confirm on
  each database connnection, as opposed to just the first.
