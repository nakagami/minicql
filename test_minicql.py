#!/usr/bin/env python3
###############################################################################
# MIT License
#
# Copyright (c) 2017,2020 Hajime Nakagami
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################
import unittest
import minicql
import decimal
import uuid
import datetime


class TestMiniCQL(unittest.TestCase):
    host = 'localhost'
    keyspace = 'test_minicql'
    user = "cassandra"
    password = "cassandra"
    port = 9042
    use_ssl = False

    def setUp(self):
        self.conn = minicql.connect(
            self.host,
            self.keyspace,
            port=self.port,
            user=self.user,
            password=self.password,
            use_ssl=self.use_ssl,
        )

    def tearDown(self):
        self.conn.close()

    def test_basic_type(self):
        cur = self.conn.cursor()
        try:
            cur.execute("drop table test_basic_type")
        except:
            pass
        cur.execute("""
            CREATE TABLE test_basic_type (
                id INT,
                s TEXT,
                dec decimal,
                d double,
                f float,
                PRIMARY KEY(id)
            )
        """)
        cur.execute("""INSERT INTO test_basic_type (id, s, dec, d, f)
            VALUES (1, NULL, 123.4, 123.4, 1.0)""")
        cur.execute("""INSERT INTO test_basic_type (id, s, dec, d, f)
            VALUES (2, 'test123', 1234.0, 1234.0, 0.125)""")
        cur.execute("""INSERT INTO test_basic_type (id, s, dec, d, f)
            VALUES (3, 'あいうえお', -0.123, -0.123, -0.125)""")

        cur.execute("SELECT id, s, dec, d, f FROM test_basic_type")
        self.assertEqual(
            set(cur.fetchall()),
            set([
                (1, None, decimal.Decimal('123.4'), 123.4, 1.0),
                (2, 'test123', decimal.Decimal('1234.0'), 1234.0, 0.125),
                (3, 'あいうえお', decimal.Decimal('-0.123'), -0.123, -0.125),
            ])
        )

        cur.execute(
            "SELECT id, s, dec, d, f FROM test_basic_type WHERE id=%s ALLOW FILTERING",
            (1,)
        )
        self.assertEqual(
            cur.fetchall(),
            [(1, None, decimal.Decimal('123.4'), 123.4, 1.0)]
        )
        cur.execute(
            "SELECT id, s, dec, d, f FROM test_basic_type WHERE s=%s ALLOW FILTERING",
            ('あいうえお',)
        )
        self.assertEqual(
            cur.fetchall(),
            [(3, 'あいうえお', decimal.Decimal('-0.123'), -0.123, -0.125)]
        )

    def test_var_type(self):
        cur = self.conn.cursor()
        try:
            cur.execute("drop table test_var_type")
        except:
            pass
        cur.execute("""
            CREATE TABLE test_var_type (
                id VARINT,
                s VARCHAR,
                PRIMARY KEY(id)
            )
        """)
        cur.execute("INSERT INTO test_var_type (id, s) VALUES (1, NULL)")
        cur.execute("INSERT INTO test_var_type (id, s) VALUES (2, 'test123')")
        cur.execute("INSERT INTO test_var_type (id, s) VALUES (3, 'あいうえお')")

        cur.execute("SELECT id, s FROM test_var_type")
        self.assertEqual(
            set(cur.fetchall()),
            set([(1, None), (2, 'test123'), (3, 'あいうえお')])
        )

    def test_uuid(self):
        cur = self.conn.cursor()
        try:
            cur.execute("drop table test_uuid_type")
        except:
            pass
        cur.execute("""
            CREATE TABLE test_uuid_type (
                id UUID,
                PRIMARY KEY(id)
            )
        """)
        cur.execute("INSERT INTO test_uuid_type (id) VALUES (uuid())")

        cur.execute("SELECT id FROM test_uuid_type")
        self.assertTrue(isinstance(cur.fetchone()[0], uuid.UUID))

    def test_date_time_type(self):
        cur = self.conn.cursor()
        try:
            cur.execute("drop table test_date_time_type")
        except:
            pass
        cur.execute("""
            CREATE TABLE test_date_time_type (
                id INT,
                dt timestamp,
                d date,
                t time,
                PRIMARY KEY(id)
            )
        """)
        cur.execute("""INSERT INTO test_date_time_type (id, dt, d, t)
            VALUES (1, '1967-08-11 12:34:56+00:00', '1967-08-11', '12:34:56.123456')""")

        cur.execute("SELECT dt, d, t FROM test_date_time_type")
        r = cur.fetchone()
        self.assertEqual(r[0], datetime.datetime(1967, 8, 11, 12, 34, 56, tzinfo=datetime.timezone.utc))
        self.assertEqual(r[1], datetime.date(1967, 8, 11))
        self.assertEqual(r[2], datetime.time(12, 34, 56, 123456))


if __name__ == "__main__":
    unittest.main()
