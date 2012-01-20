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

Require SQL Alchemy 0.7 for the events.
