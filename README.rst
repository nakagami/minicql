=========
minicql
=========

Yet another Cassandra (CQL protocol) database driver.

Requirement
--------------

- Python3.5+

Example
-------------

Simple example

::

   import minicql
   conn = minicql.connect('server_name', 'keyspace')
   cur = conn.cursor()
   cur.execute("select * from test")
   for c in cur.fetchall():
       print(c)
   conn.close()

Use Cosmos DB cassandra API

::

   import minicql
   conn = minicql.connect(
       'xxxxx.cassandra.cosmos.azure.com',
       'keyspace',
       user='xxxxx',
       password='?????',
       port=10350,
       use_ssl=True
   )
       ...

