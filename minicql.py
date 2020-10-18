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

# https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec

import socket
import ssl
import struct
import decimal
import datetime
import time
import binascii
import uuid

VERSION = (0, 2, 0)
__version__ = '%s.%s.%s' % VERSION
apilevel = '2.0'
threadsafety = 1
paramstyle = 'format'

# protocol version
REQUEST_PROTOCOL_VERSION = 0x04
RESPONSE_PROTOCOL_VERSION = 0x84

# flags
COMPRESSION_FLAG = 0x01
TRACING_FLAG = 0x02
CUSOM_PAYLOAD_FLAG = 0x04
WARNING_FLAG = 0x08
USE_BETA_FLAG = 0x10

# opcode
OP_ERROR = 0x00
OP_STARTUP = 0x01
OP_READY = 0x02
OP_AUTHENTICATE = 0x03
OP_OPTIONS = 0x05
OP_SUPPORTED = 0x06
OP_QUERY = 0x07
OP_RESULT = 0x08
OP_PREPARE = 0x09
OP_EXECUTE = 0x0A
OP_REGISTER = 0x0B
OP_EVENT = 0x0C
OP_BATCH = 0x0D
OP_AUTH_CHALLENGE = 0x0E
OP_AUTH_RESPONSE = 0x0F
OP_AUTH_SUCCESS = 0x10

CONSISTENCY_ANY = b'\x00\x00'
CONSISTENCY_ONE = b'\x00\x01'
CONSISTENCY_TWO = b'\x00\x02'
CONSISTENCY_THREE = b'\x00\x03'
CONSISTENCY_QUORUM = b'\x00\x04'
CONSISTENCY_ALL = b'\x00\x05'
CONSISTENCY_LOCAL_QUORUM = b'\x00\x06'
CONSISTENCY_EACH_QUORUM = b'\x00\x07'
CONSISTENCY_SERIAL = b'\x00\x08'
CONSISTENCY_LOCAL_SERIAL = b'\x00\x09'
CONSISTENCY_LOCAL_ONE = b'\x00\x0A'


def encode_integer(n, ln):
    b = bytearray()
    for i in range(ln):
        b.append((n >> (i*8)) & 0xff)
    b.reverse()
    return bytes(b)


def encode_string(s):
    b = s.encode('utf-8')
    if len(b) < 0xFFFF:
        return encode_integer(len(b), 2) + b
    elif len(b) < 0xFFFFFFFF:
        return encode_integer(len(b), 4) + b


def encode_long_string(s):
    b = s.encode('utf-8')
    return encode_integer(len(b), 4) + b


def encode_string_map(d):
    b = encode_integer(len(d), 2)
    for k, v in d.items():
        b += encode_string(k)
        b += encode_string(v)
    return b


def decode_varint(b):
    n = int(binascii.b2a_hex(b).decode('utf-8'), 16)
    if b[0] & 0x80:
        n -= 1 << (len(b) * 8)
    return n


def decode_int(b):
    n = int.from_bytes(b[0:4], byteorder='big', signed=True)
    return n, b[4:]


def decode_long(b):
    n = int.from_bytes(b[0:8], byteorder='big', signed=True)
    return n, b[8:]


def decode_short(b):
    n = int.from_bytes(b[0:2], byteorder='big', signed=True)
    return n, b[2:]


def decode_string(b):
    ln, b = decode_short(b)
    s = b[:ln].decode('utf-8')
    b = b[ln:]
    return s, b


def decode_long_string(b):
    ln, b = decode_int(b)
    s = b[:ln].decode('utf-8')
    b = b[ln:]
    return s, b


def decode_string_list(b):
    r = []
    ln, b = decode_short(b)
    for i in range(ln):
        s, b = decode_string(b)
        r.append(s)
    return r, b


def decode_bytes(b):
    ln, b = decode_int(b)
    if ln < 0:
        return None, b
    return b[:ln], b[ln:]


def decode_string_map(b):
    r = {}
    ln, b = decode_short(b)
    for i in range(ln):
        k, b = decode_string(b)
        v, b = decode_string(b)
        r[k] = v
    return r, b


def decode_string_multimap(b):
    r = {}
    ln, b = decode_short(b)
    for i in range(ln):
        k, b = decode_string(b)
        v, b = decode_string_list(b)
        r[k] = v
    return r, b


def decode_rows(body):
    kind, b = decode_int(body)
    assert kind == 2    # Rows
    flags, b = decode_int(b)
    column_count, b = decode_int(b)
    if flags & 0x0002:
        paging_state, b = decode_bytes(b)
    else:
        paging_state = b''
    if flags & 0x0001:
        keyspace_name, b = decode_string(b)
        table_name, b = decode_string(b)
    else:
        keyspace_name = table_name = ''

    description = []
    for i in range(column_count):
        if flags & 0x001 == 0:
            keyspace_name, b = decode_string(b)
            table_name, b = decode_string(b)
        column_name, b = decode_string(b)
        type_code, b = decode_short(b)
        sub_type = None
        if type_code == 0x0000:     # Custom
            raise ValueError("Custom type still not support")
        elif type_code == 0x0020:   # List
            sub_type, b = decode_short(b)
        elif type_code == 0x0021:   # Map
            k, b = decode_short(b)
            v, b = decode_short(b)
            sub_type = (k, v)
        elif type_code == 0x0022:   # Set
            sub_type, b = decode_short(b)
        description.append((column_name, type_code, sub_type, None, None, None, None))

    rows_count, b = decode_int(b)

    rows = []

    for i in range(rows_count):
        row = []
        for j in range(len(description)):
            raw_data, b = decode_bytes(b)
            row.append(raw_data)
        rows.append(row)

    return description, rows, paging_state


def escape_parameter(v):
    if v is None:
        return 'NULL'

    t = type(v)
    if t == str:
        return u"'" + v.replace(u"'", u"''") + u"'"
    elif t == bool:
        return u"TRUE" if v else u"FALSE"
    elif t == time.struct_time:
        return u'%04d-%02d-%02d %02d:%02d:%02d' % (
            v.tm_year, v.tm_mon, v.tm_mday, v.tm_hour, v.tm_min, v.tm_sec)
    elif t == datetime.datetime:
        return "timestamp '" + v.isoformat() + "'"
    elif t == datetime.date:
        return "date '" + str(v) + "'"
    elif t == datetime.timedelta:
        return u"interval '" + str(v) + "'"
    elif t == int or t == float:
        return str(v)
    elif t == decimal.Decimal:
        return "decimal '" + str(v) + "'"
    else:
        return "'" + str(v) + "'"

# ------------------------------------------------------------------------------
class Error(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        super().__init__()

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.message


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    def __init__(self, message):
        DatabaseError.__init__(self, -1, message)


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    def __init__(self):
        DatabaseError.__init__(self, -1, 'NotSupportedError')


class Cursor(object):
    def __init__(self, connection):
        self.connection = connection
        self.description = []
        self._rows = []
        self._rowcount = 0
        self.arraysize = 1
        self.query = None

    def __enter__(self):
        return self

    def __exit__(self, exc, value, traceback):
        self.close()

    def callproc(self, procname, args=()):
        raise NotSupportedError()

    def nextset(self, procname, args=()):
        raise NotSupportedError()

    def setinputsizes(sizes):
        pass

    def setoutputsize(size, column=None):
        pass

    def execute(self, query, args=()):
        if not self.connection or not self.connection.is_connect():
            raise ProgrammingError("Lost connection")

        self.description = []
        self.args = args
        if args:
            escaped_args = tuple(escape_parameter(arg).replace('%', '%%') for arg in args)
            query = query.replace('%', '%%').replace('%%s', '%s')
            query = query % escaped_args
            query = query.replace('%%', '%')
        self.query = query
        self.description, self._rows = self.connection._execute(query)
        self._rowcount = len(self._rows)

    def executemany(self, query, seq_of_params):
        rowcount = 0
        for params in seq_of_params:
            self.execute(query, params)
            rowcount += self._rowcount
        self._rowcount = rowcount

    def _convert_row(self, row):
        for i in range(len(row)):
            if row[i] is None:
                continue
            type_id = self.description[i][1]
            if type_id in (0x0000, 0x0001, 0x000D):     # string
                row[i] = row[i].decode('utf-8')
            elif type_id in (0x0002, 0x0005, 0x0009, 0x000E, 0x0013, 0x0014):   # integer
                row[i] = int.from_bytes(row[i], byteorder='big')
            elif type_id in (0x0003, ):     # binary
                pass
            elif type_id in (0x0004, ):     # bool
                row[i] = bool(int.from_bytes(row[i], byteorder='big'))
            elif type_id in (0x0006, ):     # decimal
                scale = int.from_bytes(row[i][:4], byteorder='big', signed=True)
                unscaled = decode_varint(row[i][4:])
                row[i] = decimal.Decimal('%de%d' % (unscaled, -scale))
            elif type_id in (0x0007, ):     # double
                row[i] = struct.unpack('>d', row[i])[0]
            elif type_id in (0x0008, ):     # double
                row[i] = struct.unpack('>f', row[i])[0]
            elif type_id in (0x000B, ):     # Timestamp
                row[i] = datetime.datetime.fromtimestamp(
                    int.from_bytes(row[i], byteorder='big', signed=True) / 1000
                )
            elif type_id in (0x000C, 0x000F):     # UUID
                row[i] = uuid.UUID(bytes=row[i])
            elif type_id in (0x0011, ):     # Date
                days = int.from_bytes(row[i], byteorder='big') - 2 ** 31
                dt = datetime.datetime(1970, 1, 1) + datetime.timedelta(days=days)
                row[i] = datetime.date(dt.year, dt.month, dt.day)
            elif type_id in (0x0012, ):     # Time
                nanosec = int.from_bytes(row[i], byteorder='big')
                microsecond = nanosec // 1000 % 1000000
                second = nanosec // 1000000000 % 60
                minute = nanosec // (60 * 1000000000) % 60
                hour = nanosec // (3600 * 1000000000)
                row[i] = datetime.time(hour, minute, second, microsecond)

        return tuple(row)

    def fetchone(self):
        if not self.connection or not self.connection.is_connect():
            raise ProgrammingError("Lost connection")
        if len(self._rows) == 0:
            return None
        return self._convert_row(self._rows.pop(0))

    def fetchmany(self, size=1):
        rs = []
        for i in range(size):
            r = self.fetchone()
            if not r:
                break
            rs.append(r)
        return rs

    def fetchall(self):
        rows = list(self._rows)
        self._rows = []
        return [self._convert_row(r) for r in rows]

    def close(self):
        self.connection = None

    @property
    def rowcount(self):
        return self._rowcount

    @property
    def closed(self):
        return self.connection is None or not self.connection.is_connect()

    def __iter__(self):
        return self

    def __next__(self):
        r = self.fetchone()
        if not r:
            raise StopIteration()
        return r

    def next(self):
        return self.__next__()


class Connection:

    def _send(self, b):
        n = 0
        while (n < len(b)):
            n += self._sock.send(b[n:])

    def _recv(self, ln):
        r = b''
        while len(r) < ln:
            b = self._sock.recv(ln-len(r))
            if not b:
                raise socket.error("Can't recv packets")
            r += b
        return r

    def _send_frame(self, opcode, body=b''):
        self._send(struct.pack(
            ">BBHBL",
            REQUEST_PROTOCOL_VERSION,
            0,
            self.stream_number,
            opcode,
            len(body),
        ))
        self._send(body)

        self.stream_number += 1
        if self.stream_number > 32768:
            self.stream_number = 0

    def _recv_frame(self):
        header = self._recv(9)
        # stream = int.from_bytes(header[2:4], byteorder='big')
        ln = int.from_bytes(header[-4:], byteorder='big')
        body = self._recv(ln)
        opcode = header[4]
        if opcode == OP_ERROR:
            n, b = decode_int(body)
            s, b = decode_string(b)
            raise OperationalError(n, s)
        return opcode, body

    def _execute(self, query):
        body = encode_long_string(query) + b'\x00\x01\x00'
        self._send_frame(OP_QUERY, body)
        opcode, body = self._recv_frame()
        kind, b = decode_int(body)
        if kind == 2:
            description, data, more_data = decode_rows(body)
        else:
            description = data = []

        return description, data

    def __init__(self, host, keyspace, port, user, password, use_ssl):
        self.host = host
        self.keyspace = keyspace
        self.port = port
        self.user = user
        self.password = password
        self.use_ssl = use_ssl
        self.stream_number = 0
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self.host, self.port))
        if self.use_ssl:
            self._sock = ssl.wrap_socket(self._sock)

        self._send_frame(OP_OPTIONS)
        opcode, body = self._recv_frame()
        assert opcode == OP_SUPPORTED
        supported_params, _ = decode_string_multimap(body)
        body = encode_string_map({'CQL_VERSION': supported_params['CQL_VERSION'][0]})
        self._send_frame(OP_STARTUP, body)
        opcode, body = self._recv_frame()

        if opcode == OP_AUTHENTICATE:
            if not (self.user and self.password):
                raise ValueError("Need credentials")
            body = b'\x00' + self.user.encode('utf-8') + b'\x00' + self.password.encode('utf-8')
            body = encode_integer(len(body), 4) + body
            self._send_frame(OP_AUTH_RESPONSE, body)
            opcode, _ = self._recv_frame()
            assert opcode == OP_AUTH_SUCCESS
        else:
            assert opcode == OP_READY

        if self.keyspace:
            self._execute("use " + keyspace)

    def __enter__(self):
        return self

    def __exit__(self, exc, value, traceback):
        self.close()

    def is_connect(self):
        return bool(self._sock)

    def cursor(self):
        return Cursor(self)

    def close(self):
        self._sock.close()
        self._sock = None


def connect(host, keyspace=None, port=9042, user=None, password=None, use_ssl=False):
    return Connection(host, keyspace, port, user, password, use_ssl)
