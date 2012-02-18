import psycopg2.extensions
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
        return "cursors.ListCursor(%s)" % psycopg2.extensions.adapt(self.query)

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

class PostgresqlConnection(psycopg2.extensions.connection):
    
    def __call__(self, q, *params):
        if q.upper().startswith('SELECT'):
            # Server-side cursors are cool. Unless we know better, use them for everything.
            cursor = self.cursor(str(uuid.uuid1()))
        else:
            cursor = self.cursor()
        cursor.execute(q, params)
        return cursor

    def cursor(self, name=None, cursor_factory=PostgresqlCursor, **kwargs):
        kwargs['cursor_factory'] = cursor_factory
        if name is not None:
            kwargs['name'] = name
        return super(PostgresqlConnection, self).cursor(**kwargs)


class SQLiteConnection(sqlite3.Connection):

    def __init__(self, *args, **kwargs):
        super(SQLiteConnection, self).__init__(*args, **kwargs)
        self.use_undocumented_c_method = False
        self.row_factory = self._row_factory

    def __call__(self, q, *params):
        if self.use_undocumented_c_method:
            return super(SQLiteConnection, self).__call__(q, *params)
        else:
            self.use_undocumented_c_method = True
            cursor = self.cursor().execute(q, params)
            self.use_undocumented_c_method = False
            return cursor

    def cursor(self, factory=SQLiteCursor):
        return super(SQLiteConnection, self).cursor(factory)

    def _row_factory(self, cursor, row):
        """
        Usage:
        con.row_factory = namedtuple_factory
        """
        # Remove any leading underscores from column names
        fields = [col[0].lstrip('_') for col in cursor.description]
        Row = collections.namedtuple("Row", fields)
        return Row(*row)


def connect(*args, **kwargs):
    engine = kwargs.get('engine', None)

    # If no engine was specified, try to guess what to do.
    if engine is None and args:
        if args[0] == ':memory:' or os.path.exists(args[0]):
            engine = 'sqlite'
        else:
            engine = 'postgresql'
            kwargs.setdefault('database', args[0])
            kwargs.setdefault('user', 'postgres')
            args = []

    if engine in ('sqlite', 'sqlite3'):
        kwargs.setdefault('factory', SQLiteConnection)
        return sqlite3.connect(*args, **kwargs)

    if engine in ('pg', 'postgres', 'postgresql', 'pscyopg', 'psycopg2'):
        kwargs.setdefault('connection_factory', PostgresqlConnection)
        return psycopg2.connect(*args, **kwargs)

        
