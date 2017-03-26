#!/usr/bin/env python3
###############################################################################
# MIT License
#
# Copyright (c) 2017 Hajime Nakagami
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


class TestMiniCQL(unittest.TestCase):
    host = 'localhost'
    keyspace = 'test'
    port = 9042

    def test_basic_type(self):
        conn = minicql.connect(self.host, self.keyspace, port=self.port)
        cur = conn.cursor()
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
            cur.fetchall(),
            [
                [1, None, decimal.Decimal('123.4'), 123.4, 1.0],
                [2, 'test123', decimal.Decimal('1234.0'), 1234.0, 0.125],
                [3, 'あいうえお', decimal.Decimal('-0.123'), -0.123, -0.125],
            ]
        )

        cur.execute(
            "SELECT id, s, dec, d, f FROM test_basic_type WHERE id=%s ALLOW FILTERING",
            (1,)
        )
        self.assertEqual(
            cur.fetchall(),
            [[1, None, decimal.Decimal('123.4'), 123.4, 1.0]]
        )
        cur.execute(
            "SELECT id, s, dec, d, f FROM test_basic_type WHERE s=%s ALLOW FILTERING",
            ('あいうえお',)
        )
        self.assertEqual(
            cur.fetchall(),
            [[3, 'あいうえお', decimal.Decimal('-0.123'), -0.123, -0.125]]
        )

        conn.close()


if __name__ == "__main__":
    unittest.main()
