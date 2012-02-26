from cursors import connect, SQLiteConnection, PostgresqlConnection
from nose.tools import eq_, assert_is_none, assert_true


class TestPostgresql(object):
    def setUp(self):
        self.c = connect(database='postgres', engine='postgres')

    def test_connect(self):
        eq_(len(self.c('SELECT * FROM pg_database LIMIT 1')), 1)
    
    def test_get_one(self):
        assert_true(hasattr(self.c('SELECT * FROM pg_database')[1], 'datname'))
    
    def test_get_range(self):
        eq_(len(self.c('SELECT * FROM pg_database')[0:2]), 2)
    
    def test_str(self):
        query = 'SELECT * FROM pg_database'
        cur = self.c(query)
        eq_(str(cur), 'cursors.PostgresqlCursor(\'DECLARE "%s" CURSOR WITHOUT HOLD FOR %s\')' % (cur.name, query))
    
    def test_tables(self):
        assert_true(len(self.c.tables()) > 1)

    def test_local_cursor(self):
        cur = self.c("SET LOCAL TIME ZONE 'America/Chicago'")
        assert_is_none(cur.name)

class TestSQLite(object):
    def setUp(self):
        self.c = connect(database=':memory:', engine='sqlite')
        self.c("CREATE TABLE people (id, name)")
        self.c("CREATE TABLE books (id, author, title)")

    def test_connect(self):
        eq_(len(self.c('SELECT * FROM sqlite_master')), 2)
    
    def test_get_one(self):
        assert_true(hasattr(self.c('SELECT * FROM sqlite_master')[1], 'name'))
    
    def test_get_range(self):
        eq_(len(self.c('SELECT * FROM sqlite_master')[0:2]), 2)
    
    def test_str(self):
        query = 'SELECT * FROM sqlite_master'
        cur = self.c(query)
        eq_(str(cur), 'cursors.SQLiteCursor(%s)' % query)
    
    def test_tables(self):
        eq_(len(self.c.tables()), 2)

def check_connection_rule(args, kwargs, engine):
    c = connect(*args, **kwargs)
    if engine == 'sqlite':
        assert_true(isinstance(c, SQLiteConnection))
    else:
        assert_true(isinstance(c, PostgresqlConnection))

def test_connection_rules():
    tests = [
        [[':memory:'], {}, 'sqlite'],
        [[], {'database': ':memory:', 'engine': 'sqlite'}, 'sqlite'],
        [['postgres'], {}, 'postgres'],
        [[], {'database': 'postgres', 'engine': 'postgres'}, 'postgres'],
    ]
    for test in tests:
        yield check_connection_rule, test[0], test[1], test[2]
