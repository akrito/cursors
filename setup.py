from setuptools import setup
import os

try:
    long_desc = open(os.path.join(os.path.dirname(__file__), 'README.md')).read()
except (IOError, OSError):
    long_desc = ''

setup(
    name = "cursors",
    version = '0.1',
    url = 'http://github.com/akrito/cursors',
    author = 'Alex Kritikos',
    author_email = 'alex@8bitb.us',
    description = 'Cursors takes SQL and gives you a sequence of named tuples.',
    long_description = long_desc,
    install_requires = ['psycopg2'],
    py_modules = ['cursors'],
)
