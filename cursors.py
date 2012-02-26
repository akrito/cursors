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
            start = index.start or 0
            self.scroll(start, mode='absolute')
            return self.fetchmany(index.stop - start)[::index.step]
        except AttributeError:
            self.scroll(index, mode='absolute')
            return self.fetchone()

    def __len__(self):
        self.scroll(0, mode='absolute')
        length = 0
        for r in self:
            length += 1
        return length

    def __iter__(self):
        # Invoking _cursor.__iter__(self) goes to infinite recursion,
        # so we do pagination by hand
        self.scroll(0, mode='absolute')
        while True:
            recs = self.fetchmany(self.itersize)
            if not recs:
                return
            for rec in recs:
                yield rec

    def __repr__(self):
        return "cursors.PostgresqlCursor(%s)" % psycopg2.extensions.adapt(self.query)

    def __str__(self):
        return self.__repr__()

    def _make_nt(self):
        return collections.namedtuple("Record", [d[0] for d in self.description or ()], rename=True)



class SQLiteCursor(sqlite3.Cursor,  collections.Sequence):

    # Based on http://stackoverflow.com/a/2322711 to make a generator
    # indexable. Previously, we cached with a deque and would re-run the query
    # and replace the cursor if the items weren't in the deque anymore, but
    # that's a lot of logic.

    def __init__(self, *args, **kwargs):
        self.cache = []
        super(SQLiteCursor, self).__init__(*args, **kwargs)

    def execute(self, q, params):
        self.query = q # XXX This doesn't include the parameters
        return super(SQLiteCursor, self).execute(q, params)

    def next(self):
        row = super(SQLiteCursor, self).next()
        self.cache.append(row)
        return row

    def __getitem__(self, index):
        try:
            max_idx = index.stop - 1
        except AttributeError:
            max_idx = index
        for n in xrange(max_idx - len(self.cache) + 1):
            self.next()
        return self.cache[index]            

    def __len__(self):
        for row in self:
            pass
        return len(self.cache)

    def __repr__(self):
        return "cursors.SQLiteCursor(%s)" % self.query

    def __str__(self):
        return self.__repr__()


# Connections

class PostgresqlConnection(psycopg2.extensions.connection):
    
    param = '%s'

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

    def tables(self):
        return self('SELECT tablename FROM pg_tables')


class SQLiteConnection(sqlite3.Connection):

    param = '?'

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
        Row = collections.namedtuple("Row", [col[0] for col in cursor.description], rename=True)
        return Row(*row)

    def tables(self):
        return self('SELECT tbl_name FROM SQLITE_MASTER')


def connect(*args, **kwargs):
    engine = kwargs.pop('engine', None)

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
