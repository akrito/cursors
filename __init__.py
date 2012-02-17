import collections
import psycopg2
import psycopg2.extras
import uuid


class ListCursor(psycopg2.extras.NamedTupleCursor, collections.Sequence):

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
        return "ListCursor('%s')" % self.query

    def __str__(self):
        return self.__repr__()


# Database connection. Call it and you get a cursor.
class D(object):

    def __init__(self, db_name, db_user='postgres'):
        self.con = psycopg2.connect('dbname=%s user=%s' % (db_name, db_user))

    def __call__(self, q, *params):
        if q.upper().startswith('SELECT'):
            # Server-side cursors are cool. Unless we know better, use them for
            # everything.
            cursor = self.con.cursor(str(uuid.uuid1()), cursor_factory=ListCursor)
        else:
            cursor = self.con.cursor()
        cursor.execute(q, params)
        return cursor
