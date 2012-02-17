import datetime
import psycopg2
import psycopg2.extras
import itertools
import uuid


class Cursor(object):

    def __init__(self, cursor, q, params):
        self.cursor = cursor
        self.q = q
        self.params = params
        self.cls = None
        self.highest = -1
        def gen():
            for r in cursor:
                yield r
        self._gen = gen()

    def __iter__(self):
        for elt in self._gen:
            self.count += 1
            yield elt

    def __getitem__(self, index):
        try:
            min_idx = index.start or 0
            max_idx = index.stop
        except AttributeError:
            min_idx = index
            max_idx = index

        if min_idx <= self.highest:
            # Reset the generator
            self._replay_query()
            # Holy recursion. This should only happen once.
            return self.__getitem__(index)

        # If there's not an off-by-one error here, I'll be amazed
        adj = self.highest + 1
        self.highest += max_idx - self.highest

        try:
            return next(itertools.islice(self._gen, index - adj, index - adj + 1))
        except TypeError:
            return list(itertools.islice(self._gen, index.start - adj, index.stop - adj, index.step))

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def _replay_query(self):
        # Create a new cursor
        con = self.cursor.connection
        cursor = con.cursor(str(uuid.uuid1()), cursor_factory=psycopg2.extras.NamedTupleCursor)
        cursor.execute(self.q, *self.params)

        # Kill the old (may be unnec.)
        self.cursor.close()

        # Replace it
        self.__init__(cursor, self.q, self.params)

    # Snark
    def __len__(self):
        raise NotImplementedError('Have you considered "COUNT(*)"?')

    def __reversed__(self):
        raise NotImplementedError('Have you considered "ORDER BY foo DESC"?')

# Database connection. Call it and you get a cursor.
class D(object):

    def __init__(self, db_name, db_user='postgres'):
        self.con = psycopg2.connect('dbname=%s user=%s' % (db_name, db_user))

    def __call__(self, q, *params):
        if q.upper().startswith('SELECT'):
            cursor = self.con.cursor(str(uuid.uuid1()), cursor_factory=psycopg2.extras.NamedTupleCursor)
        else:
            cursor = self.con.cursor()
        cursor.execute(q, *params)
        return Cursor(cursor, q, params)
