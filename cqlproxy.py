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
import sys
import os
import socket
import binascii
import threading
import datetime

import minicql


def _recv_from_sock(sock, nbytes):
    n = nbytes
    recieved = b''
    while n:
        bs = sock.recv(n)
        recieved += bs
        n -= len(bs)
    return recieved

OPCODE = {
    0x00: 'ERROR',
    0x01: 'STARTUP',
    0x02: 'READY',
    0x03: 'AUTHENTICATE',
    0x05: 'OPTIONS',
    0x06: 'SUPPORTED',
    0x07: 'QUERY',
    0x08: 'RESULT',
    0x09: 'PREPARE',
    0x0A: 'EXECUTE',
    0x0B: 'REGISTER',
    0x0C: 'EVENT',
    0x0D: 'BATCH',
    0x0E: 'AUTH_CHALLENGE',
    0x0F: 'AUTH_RESPONSE',
    0x10: 'AUTH_SUCCESS',
}


def read_frame(sock):
    header = _recv_from_sock(sock, 9)
    stream = int.from_bytes(header[2:4], byteorder='big')
    ln = int.from_bytes(header[-4:], byteorder='big')
    body = _recv_from_sock(sock, ln)
    opcode = OPCODE[header[4]]
    assert header[0] == 0x04 or header[0] == 0x84
    if header[0] == 0x04:
        version = 'C->S'
    elif header[0] == 0x84:
        version = 'S->C'
    else:
        raise ValueError('Invalid version:' + hex(header[0]))
    print('%s:flags=%d:stream=%d:%s:len=%d' % (version, header[1], stream, opcode, ln), end=' ')
    if opcode == 'STARTUP':
        d, b = minicql.decode_string_map(body)
        print(d)
    elif opcode == 'SUPPORTED':
        d, b = minicql.decode_string_multimap(body)
        print(d)
    elif opcode == 'REGISTER':
        r, b = minicql.decode_string_list(body)
        print(r)
    elif opcode == 'QUERY':
        query, b = minicql.decode_long_string(body)
        consistency = int.from_bytes(b[:2], byteorder='big')
        flags = b[2]
        b = b[3:]
        print("query=%s,consistency=%d,flags=%s" % (
            query, consistency, hex(flags)
        ), binascii.b2a_hex(b).decode('utf-8'))
        if flags & 0x01:
            num_params = int.from_bytes(b[:2], byteorder='big')
            b = b[2:]
        if flags & 0x04:
            result_page_size = int.from_bytes(b[:2], byteorder='big')
            b = b[2:]
            print('result_page_size=', result_page_size)
        if flags & 0x08:
            paging_state = b[:2]
            b = b[2:]
            print('page_state=', binascii.b2a_hex(serial_consistency).decode('utf-8'))
        if flags & 0x10:
            serial_consistency = int.from_bytes(b[:2], byteorder='big')
            b = b[2:]
            print('serial_consistency=', serial_consistency)
        if flags & 0x20:
            t = b[:8]
            b = b[8:]
            print('timestamp=', t)
    elif opcode == 'ERROR':
        n, b = minicql.decode_int(body)
        s, b = minicql.decode_string(b)
        print('%s:"%s"' % (hex(n), s))
    elif opcode == 'RESULT':
        kind, b = minicql.decode_int(body)
        if kind == 2:
            description, data, more_data = minicql.decode_rows(body)
            print(description)
            print(data)
            print(more_data)
        else:
            print('kind=%d' % (kind,))
            print(b)
    else:
        print(binascii.b2a_hex(body).decode('utf-8'))
    return header + body


def relay_packets(client_sock, server_sock):
    while True:
        b = read_frame(client_sock)
        server_sock.send(b)
        b = read_frame(server_sock)
        client_sock.send(b)


def proxy_wire(server_name, server_port, listen_host, listen_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((listen_host, listen_port))
    sock.listen(1)

    while True:
        client_sock, addr = sock.accept()
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.connect((server_name, server_port))
        threading.Thread(target=relay_packets, args=(client_sock, server_sock)).start()


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage : ' + sys.argv[0] + ' server[:port] [listen_host:]listen_port')
        sys.exit()

    server = sys.argv[1].split(':')
    server_name = server[0]
    if len(server) == 1:
        server_port = 9042
    else:
        server_port = int(server[1])

    listen = sys.argv[2].split(':')
    if len(listen) == 1:
        listen_host = 'localhost'
        listen_port = int(listen[0])
    else:
        listen_host = listen[0]
        listen_port = int(listen[1])

    print(server_name, server_port, listen_host, listen_port)
    proxy_wire(server_name, server_port, listen_host, listen_port)
