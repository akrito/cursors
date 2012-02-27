Cursors
=======

I like SQL. I want to give the computer SQL and get back a list of
objects. This does that, mostly.

Is it safe? [![Build Status](https://secure.travis-ci.org/akrito/cursors.png?branch=master)](http://travis-ci.org/akrito/cursors)

Example
=======

Cursors are sequences of named tuples. Here's how you get one:

    from cursors import connect
    connection  = connect('bang')
    cursor = connection('SELECT * FROM business_business ORDER BY id')

Since these connections and cursors are subclasses of actual connections and
cursors, you get all those other methods for free.
