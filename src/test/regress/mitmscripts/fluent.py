import logging
import os
import queue
import re
import signal
import socket
import struct
import threading
import time
import traceback
from itertools import count

import structs
from construct.lib import ListContainer
from mitmproxy import ctx, tcp

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.DEBUG)

# I. Command Strings


class Handler:
    """
    This class hierarchy serves two purposes:
    1. Allow command strings to be evaluated. Once evaluated you'll have a Handler you can
       pass packets to
    2. Process packets as they come in and decide what to do with them.

    Subclasses which want to change how packets are handled should override _handle.
    """

    def __init__(self, root=None):
        # all packets are first sent to the root handler to be processed
        self.root = root if root else self
        # all handlers keep track of the next handler so they know where to send packets
        self.next = None

    def _accept(self, flow, message):
        result = self._handle(flow, message)

        if result == "pass":
            # defer to our child
            if not self.next:
                raise Exception("we don't know what to do!")

            if self.next._accept(flow, message) == "stop":
                if self.root is not self:
                    return "stop"
                self.next = KillHandler(self)
                flow.kill()
        else:
            return result

    def _handle(self, flow, message):
        """
        Handlers can return one of three things:
        - "done" tells the parent to stop processing. This performs the default action,
          which is to allow the packet to be sent.
        - "pass" means to delegate to self.next and do whatever it wants
        - "stop" means all processing will stop, and all connections will be killed
        """
        # subclasses must implement this
        raise NotImplementedError()


class FilterableMixin:
    def contains(self, pattern):
        self.next = Contains(self.root, pattern)
        return self.next

    def matches(self, pattern):
        self.next = Matches(self.root, pattern)
        return self.next

    def after(self, times):
        self.next = After(self.root, times)
        return self.next

    def __getattr__(self, attr):
        """
        Methods such as .onQuery trigger when a packet with that name is intercepted

        Adds support for commands such as:
          conn.onQuery(query="COPY")

        Returns a function because the above command is resolved in two steps:
          conn.onQuery becomes conn.__getattr__("onQuery")
          conn.onQuery(query="COPY") becomes conn.__getattr__("onQuery")(query="COPY")
        """
        if attr.startswith("on"):

            def doit(**kwargs):
                self.next = OnPacket(self.root, attr[2:], kwargs)
                return self.next

            return doit
        raise AttributeError


class ActionsMixin:
    def kill(self):
        self.next = KillHandler(self.root)
        return self.next

    def allow(self):
        self.next = AcceptHandler(self.root)
        return self.next

    def killall(self):
        self.next = KillAllHandler(self.root)
        return self.next

    def reset(self):
        self.next = ResetHandler(self.root)
        return self.next

    def cancel(self, pid):
        self.next = CancelHandler(self.root, pid)
        return self.next

    def connect_delay(self, timeMs):
        self.next = ConnectDelayHandler(self.root, timeMs)
        return self.next


class AcceptHandler(Handler):
    def __init__(self, root):
        super().__init__(root)

    def _handle(self, flow, message):
        return "done"


class KillHandler(Handler):
    def __init__(self, root):
        super().__init__(root)

    def _handle(self, flow, message):
        flow.kill()
        return "done"


class KillAllHandler(Handler):
    def __init__(self, root):
        super().__init__(root)

    def _handle(self, flow, message):
        return "stop"


class ResetHandler(Handler):
    # try to force a RST to be sent, something went very wrong!
    def __init__(self, root):
        super().__init__(root)

    def _handle(self, flow, message):
        flow.kill()  # tell mitmproxy this connection should be closed

        # this is a mitmproxy.connections.ClientConnection(mitmproxy.tcp.BaseHandler)
        client_conn = flow.client_conn
        # this is a regular socket object
        conn = client_conn.connection

        # cause linux to send a RST
        LINGER_ON, LINGER_TIMEOUT = 1, 0
        conn.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_LINGER,
            struct.pack("ii", LINGER_ON, LINGER_TIMEOUT),
        )
        conn.close()

        # closing the connection isn't ideal, this thread later crashes when mitmproxy
        # tries to call conn.shutdown(), but there's nothing else to clean up so that's
        # maybe okay

        return "done"


class CancelHandler(Handler):
    "Send a SIGINT to the process"

    def __init__(self, root, pid):
        super().__init__(root)
        self.pid = pid

    def _handle(self, flow, message):
        os.kill(self.pid, signal.SIGINT)
        # give the signal a chance to be received before we let the packet through
        time.sleep(0.1)
        return "done"


class ConnectDelayHandler(Handler):
    "Delay the initial packet by sleeping before deciding what to do"

    def __init__(self, root, timeMs):
        super().__init__(root)
        self.timeMs = timeMs

    def _handle(self, flow, message):
        if message.is_initial:
            time.sleep(self.timeMs / 1000.0)
        return "done"


class Contains(Handler, ActionsMixin, FilterableMixin):
    def __init__(self, root, pattern):
        super().__init__(root)
        self.pattern = pattern

    def _handle(self, flow, message):
        if self.pattern in message.content:
            return "pass"
        return "done"


class Matches(Handler, ActionsMixin, FilterableMixin):
    def __init__(self, root, pattern):
        super().__init__(root)
        self.pattern = re.compile(pattern)

    def _handle(self, flow, message):
        if self.pattern.search(message.content):
            return "pass"
        return "done"


class After(Handler, ActionsMixin, FilterableMixin):
    "Don't pass execution to our child until we've handled 'times' messages"

    def __init__(self, root, times):
        super().__init__(root)
        self.target = times

    def _handle(self, flow, message):
        if not hasattr(flow, "_after_count"):
            flow._after_count = 0

        if flow._after_count >= self.target:
            return "pass"

        flow._after_count += 1
        return "done"


class OnPacket(Handler, ActionsMixin, FilterableMixin):
    """Triggers when a packet of the specified kind comes around"""

    def __init__(self, root, packet_kind, kwargs):
        super().__init__(root)
        self.packet_kind = packet_kind
        self.filters = kwargs

    def _handle(self, flow, message):
        if not message.parsed:
            # if this is the first message in the connection we just skip it
            return "done"
        for msg in message.parsed:
            typ = structs.message_type(msg, from_frontend=message.from_client)
            if typ == self.packet_kind:
                matches = structs.message_matches(
                    msg, self.filters, message.from_client
                )
                if matches:
                    return "pass"
        return "done"


class RootHandler(Handler, ActionsMixin, FilterableMixin):
    def _handle(self, flow, message):
        # do whatever the next Handler tells us to do
        return "pass"


class RecorderCommand:
    def __init__(self):
        self.root = self
        self.command = None

    def dump(self):
        # When the user calls dump() we return everything we've captured
        self.command = "dump"
        return self

    def reset(self):
        # If the user calls reset() we dump all captured packets without returning them
        self.command = "reset"
        return self


# II. Utilities for interfacing with mitmproxy


def build_handler(spec):
    "Turns a command string into a RootHandler ready to accept packets"
    root = RootHandler()
    recorder = RecorderCommand()
    handler = eval(spec, {"__builtins__": {}}, {"conn": root, "recorder": recorder})
    return handler.root


# a bunch of globals

handler = None  # the current handler used to process packets
command_thread = None  # sits on the fifo and waits for new commands to come in
captured_messages = queue.Queue()  # where we store messages used for recorder.dump()
connection_count = count()  # so we can give connections ids in recorder.dump()


def listen_for_commands(fifoname):
    def emit_row(conn, from_client, message):
        # we're using the COPY text format. It requires us to escape backslashes
        cleaned = message.replace("\\", "\\\\")
        source = "coordinator" if from_client else "worker"
        return "{}\t{}\t{}".format(conn, source, cleaned)

    def emit_message(message):
        if message.is_initial:
            return emit_row(
                message.connection_id, message.from_client, "[initial message]"
            )

        pretty = structs.print(message.parsed)
        return emit_row(message.connection_id, message.from_client, pretty)

    def all_items(queue_):
        "Pulls everything out of the queue without blocking"
        try:
            while True:
                yield queue_.get(block=False)
        except queue.Empty:
            pass

    def drop_terminate_messages(messages):
        """
        Terminate() messages happen eventually, Citus doesn't feel any need to send them
        immediately, so tests which embed them aren't reproducible and fail to timing
        issues. Here we simply drop those messages.
        """

        def isTerminate(msg, from_client):
            kind = structs.message_type(msg, from_client)
            return kind == "Terminate"

        for message in messages:
            if not message.parsed:
                yield message
                continue
            message.parsed = ListContainer(
                [
                    msg
                    for msg in message.parsed
                    if not isTerminate(msg, message.from_client)
                ]
            )
            message.parsed.from_frontend = message.from_client
            if len(message.parsed) == 0:
                continue
            yield message

    def handle_recorder(recorder):
        global connection_count
        result = ""

        if recorder.command == "reset":
            result = ""
            connection_count = count()
        elif recorder.command != "dump":
            # this should never happen
            raise Exception("Unrecognized command: {}".format(recorder.command))

        results = []
        messages = all_items(captured_messages)
        messages = drop_terminate_messages(messages)
        for message in messages:
            if recorder.command == "reset":
                continue
            results.append(emit_message(message))
        result = "\n".join(results)

        logging.debug("about to write to fifo")
        with open(fifoname, mode="w") as fifo:
            logging.debug("successfully opened the fifo for writing")
            fifo.write("{}".format(result))

    while True:
        logging.debug("about to read from fifo")
        with open(fifoname, mode="r") as fifo:
            logging.debug("successfully opened the fifo for reading")
            slug = fifo.read()
            logging.info("received new command: %s", slug.rstrip())

        try:
            handler = build_handler(slug)
            if isinstance(handler, RecorderCommand):
                handle_recorder(handler)
                continue
        except Exception as e:
            traceback.print_exc()
            result = str(e)
        else:
            result = None

        if not result:
            try:
                ctx.options.update(slug=slug)
            except Exception as e:
                result = str(e)
            else:
                result = ""

        logging.debug("about to write to fifo")
        with open(fifoname, mode="w") as fifo:
            logging.debug("successfully opened the fifo for writing")
            fifo.write("{}\n".format(result))
            logging.info("responded to command: %s", result.split("\n")[0])


def create_thread(fifoname):
    global command_thread

    if not fifoname:
        return
    if not len(fifoname):
        return

    if command_thread:
        print("cannot change the fifo path once mitmproxy has started")
        return

    command_thread = threading.Thread(
        target=listen_for_commands, args=(fifoname,), daemon=True
    )
    command_thread.start()


# III. mitmproxy callbacks


def load(loader):
    loader.add_option("slug", str, "conn.allow()", "A script to run")
    loader.add_option("fifo", str, "", "Which fifo to listen on for commands")


def configure(updated):
    global handler

    if "slug" in updated:
        text = ctx.options.slug
        handler = build_handler(text)

    if "fifo" in updated:
        fifoname = ctx.options.fifo
        create_thread(fifoname)


def tcp_message(flow: tcp.TCPFlow):
    """
    This callback is hit every time mitmproxy receives a packet. It's the main entrypoint
    into this script.
    """
    global connection_count

    tcp_msg = flow.messages[-1]

    # Keep track of all the different connections, assign a unique id to each
    if not hasattr(flow, "connection_id"):
        flow.connection_id = next(connection_count)
    tcp_msg.connection_id = flow.connection_id

    # The first packet the frontend sends shounld be parsed differently
    tcp_msg.is_initial = len(flow.messages) == 1

    if tcp_msg.is_initial:
        # skip parsing initial messages for now, they're not important
        tcp_msg.parsed = None
    else:
        tcp_msg.parsed = structs.parse(
            tcp_msg.content, from_frontend=tcp_msg.from_client
        )

    # record the message, for debugging purposes
    captured_messages.put(tcp_msg)

    # okay, finally, give the packet to the command the user wants us to use
    handler._accept(flow, tcp_msg)
