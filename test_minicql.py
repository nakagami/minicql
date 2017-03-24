#!/usr/bin/env python3
# coding:utf-8
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


class TestMiniCQL(unittest.TestCase):
    host = 'localhost'
    keyspace = 'test'
    port = 9042

    def setUp(self):
        conn = minicql.connect(self.host, self.keyspace, port=self.port)
        cur = conn.cursor()
        try:
            cur.execute("drop table test")
        except:
            pass
        cur.execute("""
            CREATE TABLE test (
                id INT,
                s TEXT,
                PRIMARY KEY(id)
            )
        """)
        cur.execute("INSERT INTO test (id, s) VALUES (1, NULL)")
        cur.execute("INSERT INTO test (id, s) VALUES (2, 'test123')")
        cur.execute("INSERT INTO test (id, s) VALUES (3, 'あいうえお')")
        conn.close()

    def test_cql(self):
        conn = minicql.connect(self.host, self.keyspace, port=self.port)
        cur = conn.cursor()
        cur.execute("SELECT * FROM test")
        self.assertEqual(
            cur.fetchall(),
            [
                [1, None],
                [2, 'test123'],
                [3, 'あいうえお'],
            ]
        )
        conn.close()


if __name__ == "__main__":
    unittest.main()
