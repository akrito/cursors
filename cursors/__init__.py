from psycopg2.extensions import adapt
from psycopg2.extras import NamedTupleCursor
import collections
import os
import psycopg2
import sqlite3
import uuid

# Cursors

class PostgresqlCursor(NamedTupleCursor, collections.Sequence):

    def __getitem__(self, index):
        try:
            self.scroll(index.start or 0, mode='absolute')
            return self.fetchmany(index.stop - index.start)[::index.step]
        except AttributeError:
            self.scroll(index, mode='absolute')
            return self.fetchone()

    def __len__(self):
        self.scroll(0, mode='absolute')
        length = 0
        for r in self:
            length += 1
        return length

    def __repr__(self):
        return "cursors.ListCursor(%s)" % adapt(self.query)

    def __str__(self):
        return self.__repr__()


class SQLiteCursor(sqlite3.Cursor,  collections.Sequence):

    # Based on http://stackoverflow.com/a/2322711 to make a generator
    # indexable. Previously, we cached with a deque and would re-run the query
    # and replace the cursor if the items weren't in the deque anymore, but
    # that's a lot of logic.

    def __init__(self, *args, **kwargs):
        self.cache = []
        super(SQLiteCursor, self).__init__(*args, **kwargs)

    def next(self):
        row = super(SQLiteCursor, self).next()
        self.cache.append(row)
        return row

    def __getitem__(self, index):
        try:
            max_idx = index.stop
        except AttributeError:
            max_idx = index
        for n in xrange(max_idx - len(self.cache) + 1):
            self.next()
        return self.cache[index]            

    def __len__(self):
        for row in self:
            pass
        return len(self.cache)


# Connections

class PostgresqlConnection(object):
    
    def __init__(self, **kwargs):
        if 'user' not in kwargs:
            kwargs['user'] = 'postgres'
        self.con = psycopg2.connect('dbname=%s user=%s' % (kwargs['name'], kwargs['user']))

    def cursor_execute(self, q, *params):
        if q.upper().startswith('SELECT'):
            # Server-side cursors are cool. Unless we know better, use them for everything.
            cursor = self.con.cursor(str(uuid.uuid1()), cursor_factory=PostgresqlCursor)
        else:
            cursor = self.con.cursor()
        cursor.execute(q, params)
        return cursor


class SQLiteConnection(object):

    def __init__(self, name):
        self.con = sqlite3.connect(name)
        self.con.row_factory = self.namedtuple_factory

    def cursor_execute(self, q, *params):
        cursor = self.con.cursor(SQLiteCursor)
        cursor.execute(q, params)
        cursor.query = q
        return cursor

    def namedtuple_factory(self, cursor, row):
        """
        Usage:
        con.row_factory = namedtuple_factory
        """
        # Remove any leading underscores from column names
        fields = [col[0].lstrip('_') for col in cursor.description]
        Row = collections.namedtuple("Row", fields)
        return Row(*row)


class D(object):
    """
    Callable database connection. Call it and you get a cursor.
    
    """

    def __init__(self, name='', engine=None, **kwargs):
        if engine == 'sqlite' or (engine is None and os.path.exists(name)):
            self.con = SQLiteConnection(name)
        else:
            self.con = PostgresqlConnection(name, **kwargs)

    def __call__(self, q, *params):
        return self.con.cursor_execute(q, *params)
