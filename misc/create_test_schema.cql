DROP KEYSPACE IF EXISTS test_minicql;

CREATE KEYSPACE test_minicql WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor' : 1
};
