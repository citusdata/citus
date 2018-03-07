#!/usr/bin/env python3

'''
A daemon which communicates with gdb.

Sits on a domain socket. Accepts commands from it and forwards them to gdb

TODO:
- prevent there from being multiple clients?
'''

import enum
import fcntl
import functools
import os
import re
import select
import signal
import socket
import subprocess
import sys

def spawn_gdb():
    process = subprocess.Popen(
        ["gdb"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # redirect stderr -> stdout

        #  these parameters ensure we're getting TextIOWrapper's and can call readline()
        universal_newlines=True,
        bufsize=1  # enables line-buffering
    )

    # prevent our calls to readline() from blocking forever
    def setnonblocking(fd):
        flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    setnonblocking(process.stdin.fileno())
    setnonblocking(process.stdout.fileno())

    return process

def listen_socket():
    # you can run a client manually with "nc -U gdb.sock"

    filename = '/home/brian/Work/citus/src/test/regress/gdb.sock'

    # before attempting to use this socket make sure it doesn't already exist
    try:
        os.unlink(filename)
    except FileNotFoundError:
        pass

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.setblocking(False)
    sock.bind(filename)
    sock.listen(1)  # we only expect one client so a backlog of 1 is sufficient

    os.chmod(filename, 0o666)  # allow non-root users to connect to our socket

    return sock

def accept_gdb_startup(stream, state):
    nextline = stream.readline()
    while len(nextline):
        print('[gdb] {}'.format(nextline.rstrip()))
        nextline = stream.readline()

def accept_client_connection(sock, state):
    conn, _ = sock.accept()
    stream = conn.makefile(mode='r',buffering=1)

    state.poll.register(conn.fileno(), select.POLLIN | select.POLLHUP)
    state.callbacks[(conn.fileno(), select.POLLIN)] = functools.partial(accept_command_from_client, stream)
    state.callbacks[(conn.fileno(), select.POLLHUP)] = functools.partial(accept_client_disconnect, conn)

def accept_command_from_client(stream, state):
    'Takes input from the client and forwards it to gdb'
    nextline = stream.readline()
    if nextline == '':
        # probably someone hitting ctrl-D
        disconnect = state.callbacks[(stream.fileno(), select.POLLHUP)]
        disconnect(state)
        return

    breakre = '!break ([a-zA-Z]+)'
    nestedcancelre = '!nested-cancel ([a-zA-Z]+) ([a-zA-Z]+)'
    if re.match(breakre, nextline):
        location = re.match(breakre, nextline).groups()[0]
        state.gdb_break(location)
    elif re.match(nestedcancelre, nextline):
        first, second = re.match(nestedcancelre, nextline).groups()
        state.gdb_nested_cancel(first, second)
    elif nextline == '\x03\n' or nextline == '!interrupt\n':
        print('[client ctrl-c] sending signal to gdb')
        state.gdb.send_signal(signal.SIGINT)
    else:
        # forward the command to gdb
        print('[client] {}'.format(nextline.rstrip()))
        state.send_gdb_command(nextline.rstrip())

def accept_client_disconnect(conn, state):
    print('client disconnected!')

    state.poll.unregister(conn.fileno())
    del state.callbacks[(conn.fileno(), select.POLLIN)]
    del state.callbacks[(conn.fileno(), select.POLLHUP)]
    conn.shutdown(socket.SHUT_RDWR)  # inform the remote end that we're done

def accept_gdb_disconnect(state):
    raise Exception('gdb disconnected')

class State:
    def __init__(self, gdb=None, poll=None, callbacks=None):
        self.gdb = gdb
        self.poll = poll if poll is not None else select.poll()
        self.callbacks = callbacks if callbacks is not None else {}

    def send_gdb_command(self, command):
        print('[gdb>] {}'.format(command))
        self.gdb.stdin.write('{}\n'.format(command))

    def gdb_attach(self, pid):
        self.send_gdb_command('attach {}'.format(pid))

    def gdb_break(self, location):
        self.send_gdb_command('''
            handle SIGUSR1 noprint
            break {location}
            commands
              delete breakpoints
              call DirectFunctionCall1Coll(pg_advisory_lock_int8, 0, 1)
              continue
            end
        '''.format(location=location))

    def gdb_nested_cancel(self, first, second):
        '''
        Will send a SIGINT to the process the first time {second} is called after the
        first time {first} is called
        '''
        self.send_gdb_command('''
            break {first}
            commands
              delete breakpoints
              break {second}
              commands
                delete breakpoints
                signal SIGINT
              end
              continue
            end
        '''.format(first=first, second=second))

    def gdb_continue(self):
        self.send_gdb_command('continue')

if __name__ == '__main__':
    poll = select.poll()

    # never call close(); the OS will do that for us when we exit
    process = spawn_gdb()
    clientsock = listen_socket()

    poll.register(process.stdout.fileno(), select.POLLIN)
    poll.register(clientsock, select.POLLIN)

    callbacks = {
        (process.stdout.fileno(), select.POLLIN): functools.partial(accept_gdb_startup, process.stdout),
        (clientsock.fileno(), select.POLLIN): functools.partial(accept_client_connection, clientsock),
        (process.stdout.fileno(), select.POLLHUP): accept_gdb_disconnect
    }

    state = State(gdb=process, poll=poll, callbacks=callbacks)

    while True:
        readies = poll.poll(1000 * 60)  # timeout expressed in milliseconds
        if not len(readies):
            print('nothing happened for a while, giving up')
            break

        readyfd, event = ready = readies[0]

        if event == (select.POLLIN | select.POLLHUP):
            print('mangled the event, dropping POLLIN')
            event = select.POLLHUP
            ready = (readyfd, event)

        try:
            callback = callbacks[ready]
        except KeyError:
            raise Exception('could not find callback for {}'.format(ready))
        callback(state)
