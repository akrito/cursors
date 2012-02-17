import datetime
import psycopg2
import psycopg2.extras
import itertools
import uuid


class Cursor(object):

    def __init__(self, cursor, q, params, maxlen=1000):
        self.cursor = cursor
        self.q = q
        self.params = params
        self.cls = None
        self.highest = -1
        # We're using the deque as a cache.
        self.deque = deque(maxlen=maxlen)
        self.maxlen = maxlen
        def gen():
            for r in cursor:
                self.highest += 1
                self.deque.append(r)
                yield r
        self._gen = gen()

    def __iter__(self):
        for elt in self._gen:
            yield elt

    def __getitem__(self, index):
        try:
            min_idx = index.start or 0
            max_idx = index.stop
            step = index.step
            single = False
        except AttributeError:
            # We were just handed one value. That's ok. Make it a slice anyway.
            min_idx = index
            max_idx = index + 1
            step = 1
            single = True

        if min_idx <= self.highest:
            self.cursor.scroll(0, mode='absolute')
            self.highest = -1
            # TODO remove copypasta
            def gen():
                for r in self.cursor:
                    self.highest += 1
                    self.deque.append(r)
                    yield r
            self._gen = gen()

        # If there's not an off-by-one error here, I'll be amazed
        adj = self.highest + 1

        try:
            return next(itertools.islice(self._gen, index - adj, index - adj + 1))
        except TypeError:
            return list(itertools.islice(self._gen, index.start - adj, index.stop - adj, index.step))

    def __getattr__(self, name):
        return getattr(self.cursor, name)

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
            # Server-side cursors are cool. Unless we know better, use them for
            # everything.
            cursor = self.con.cursor(str(uuid.uuid1()), cursor_factory=psycopg2.extras.NamedTupleCursor)
        else:
            cursor = self.con.cursor()
        cursor.execute(q, *params)
        return Cursor(cursor, q, params)
