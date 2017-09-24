=========
minicql
=========

Yet another Cassandra (CQL protocol) database driver.

Requirement
--------------

- Python3.5+

Example
-------------

::

   import minicql
   conn = minicql.connect('server_name', 'keyspace')
   cur = conn.cursor()
   cur.execute("select * from test")
   for c in cur.fetchall():
       print(c)
   conn.close()

