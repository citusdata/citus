import re

import construct.lib as cl
from construct import (
    Array,
    Byte,
    Bytes,
    Computed,
    CString,
    Enum,
    GreedyBytes,
    GreedyRange,
    Int8ub,
    Int16sb,
    Int16ub,
    Int32sb,
    Int32ub,
    RestreamData,
    Struct,
    Switch,
    this,
)

# For all possible message formats see:
# https://www.postgresql.org/docs/current/protocol-message-formats.html


class MessageMeta(type):
    def __init__(cls, name, bases, namespace):
        """
        __init__ is called every time a subclass of MessageMeta is declared
        """
        if not hasattr(cls, "_msgtypes"):
            raise Exception(
                "classes which use MessageMeta must have a '_msgtypes' field"
            )

        if not hasattr(cls, "_classes"):
            raise Exception(
                "classes which use MessageMeta must have a '_classes' field"
            )

        if not hasattr(cls, "struct"):
            # This is one of the direct subclasses
            return

        if cls.__name__ in cls._classes:
            raise Exception(
                "You've already made a class called {}".format(cls.__name__)
            )
        cls._classes[cls.__name__] = cls

        # add a _type field to the struct so we can identify it while printing structs
        cls.struct = cls.struct + ("_type" / Computed(name))

        if not hasattr(cls, "key"):
            return

        # register the type, so we can tell the parser about it
        key = cls.key
        if key in cls._msgtypes:
            raise Exception(
                "key {} is already assigned to {}".format(
                    key, cls._msgtypes[key].__name__
                )
            )
        cls._msgtypes[key] = cls


class Message:
    "Do not subclass this object directly. Instead, subclass of one of the below types"

    def print(message):
        "Define this on subclasses you want to change the representation of"
        raise NotImplementedError

    def typeof(message):
        "Define this on subclasses you want to change the expressed type of"
        return message._type

    @classmethod
    def _default_print(cls, name, msg):
        recur = cls.print_message
        return "{}({})".format(
            name,
            ",".join(
                "{}={}".format(key, recur(value))
                for key, value in msg.items()
                if not key.startswith("_")
            ),
        )

    @classmethod
    def find_typeof(cls, msg):
        if not hasattr(cls, "_msgtypes"):
            raise Exception("Do not call this method on Message, call it on a subclass")
        if isinstance(msg, cl.ListContainer):
            raise ValueError("do not call this on a list of messages")
        if not isinstance(msg, cl.Container):
            raise ValueError("must call this on a parsed message")
        if not hasattr(msg, "_type"):
            return "Anonymous"
        if msg._type and msg._type not in cls._classes:
            return msg._type
        return cls._classes[msg._type].typeof(msg)

    @classmethod
    def print_message(cls, msg):
        if not hasattr(cls, "_msgtypes"):
            raise Exception("Do not call this method on Message, call it on a subclass")

        if isinstance(msg, cl.ListContainer):
            return repr([cls.print_message(message) for message in msg])

        if not isinstance(msg, cl.Container):
            return msg

        if not hasattr(msg, "_type"):
            return cls._default_print("Anonymous", msg)

        if msg._type and msg._type not in cls._classes:
            return cls._default_print(msg._type, msg)

        try:
            return cls._classes[msg._type].print(msg)
        except NotImplementedError:
            return cls._default_print(msg._type, msg)

    @classmethod
    def name_to_struct(cls):
        return {_class.__name__: _class.struct for _class in cls._msgtypes.values()}

    @classmethod
    def name_to_key(cls):
        return {_class.__name__: ord(key) for key, _class in cls._msgtypes.items()}


class SharedMessage(Message, metaclass=MessageMeta):
    "A message which could be sent by either the frontend or the backend"
    _msgtypes = dict()
    _classes = dict()


class FrontendMessage(Message, metaclass=MessageMeta):
    "A message which will only be sent be a backend"
    _msgtypes = dict()
    _classes = dict()


class BackendMessage(Message, metaclass=MessageMeta):
    "A message which will only be sent be a frontend"
    _msgtypes = dict()
    _classes = dict()


class Query(FrontendMessage):
    key = "Q"
    struct = Struct("query" / CString("ascii"))

    @staticmethod
    def print(message):
        query = message.query
        query = Query.normalize_shards(query)
        query = Query.normalize_timestamps(query)
        query = Query.normalize_assign_txn_id(query)
        return "Query(query={})".format(query)

    @staticmethod
    def normalize_shards(content):
        """
        For example:
        >>> normalize_shards(
        >>>   'COPY public.copy_test_120340 (key, value) FROM STDIN WITH (FORMAT BINARY))'
        >>> )
        'COPY public.copy_test_XXXXXX (key, value) FROM STDIN WITH (FORMAT BINARY))'
        """
        result = content
        pattern = re.compile(r"public\.[a-z_]+(?P<shardid>[0-9]+)")
        for match in pattern.finditer(content):
            span = match.span("shardid")
            replacement = "X" * (span[1] - span[0])
            result = result[: span[0]] + replacement + result[span[1] :]
        return result

    @staticmethod
    def normalize_timestamps(content):
        """
        For example:
        >>> normalize_timestamps('2018-06-07 05:18:19.388992-07')
        'XXXX-XX-XX XX:XX:XX.XXXXXX-XX'
        >>> normalize_timestamps('2018-06-11 05:30:43.01382-07')
        'XXXX-XX-XX XX:XX:XX.XXXXXX-XX'
        """

        pattern = re.compile(
            "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{2,6}-[0-9]{2}"
        )

        return re.sub(pattern, "XXXX-XX-XX XX:XX:XX.XXXXXX-XX", content)

    @staticmethod
    def normalize_assign_txn_id(content):
        """
        For example:
        >>> normalize_assign_txn_id('SELECT assign_distributed_transaction_id(0, 52, ...')
        'SELECT assign_distributed_transaction_id(0, XX, ...'
        """

        pattern = re.compile(
            r"assign_distributed_transaction_id\s*\("  # a method call
            r"\s*[0-9]+\s*,"  # an integer first parameter
            r"\s*(?P<transaction_id>[0-9]+)"  # an integer second parameter
        )
        result = content
        for match in pattern.finditer(content):
            span = match.span("transaction_id")
            result = result[: span[0]] + "XX" + result[span[1] :]
        return result


class Terminate(FrontendMessage):
    key = "X"
    struct = Struct()


class CopyData(SharedMessage):
    key = "d"
    struct = Struct(
        "data" / GreedyBytes  # reads all of the data left in this substream
    )


class CopyDone(SharedMessage):
    key = "c"
    struct = Struct()


class EmptyQueryResponse(BackendMessage):
    key = "I"
    struct = Struct()


class CopyOutResponse(BackendMessage):
    key = "H"
    struct = Struct(
        "format" / Int8ub,
        "columncount" / Int16ub,
        "columns" / Array(this.columncount, Struct("format" / Int16ub)),
    )


class ReadyForQuery(BackendMessage):
    key = "Z"
    struct = Struct(
        "state"
        / Enum(
            Byte,
            idle=ord("I"),
            in_transaction_block=ord("T"),
            in_failed_transaction_block=ord("E"),
        )
    )


class CommandComplete(BackendMessage):
    key = "C"
    struct = Struct("command" / CString("ascii"))


class RowDescription(BackendMessage):
    key = "T"
    struct = Struct(
        "fieldcount" / Int16ub,
        "fields"
        / Array(
            this.fieldcount,
            Struct(
                "_type" / Computed("F"),
                "name" / CString("ascii"),
                "tableoid" / Int32ub,
                "colattrnum" / Int16ub,
                "typoid" / Int32ub,
                "typlen" / Int16sb,
                "typmod" / Int32sb,
                "format_code" / Int16ub,
            ),
        ),
    )


class DataRow(BackendMessage):
    key = "D"
    struct = Struct(
        "_type" / Computed("data_row"),
        "columncount" / Int16ub,
        "columns"
        / Array(
            this.columncount,
            Struct(
                "_type" / Computed("C"),
                "length" / Int16sb,
                "value" / Bytes(this.length),
            ),
        ),
    )


class AuthenticationOk(BackendMessage):
    key = "R"
    struct = Struct()


class ParameterStatus(BackendMessage):
    key = "S"
    struct = Struct(
        "name" / CString("ASCII"),
        "value" / CString("ASCII"),
    )

    def print(message):
        name, value = ParameterStatus.normalize(message.name, message.value)
        return "ParameterStatus({}={})".format(name, value)

    @staticmethod
    def normalize(name, value):
        if name in ("TimeZone", "server_version"):
            value = "XXX"
        return (name, value)


class BackendKeyData(BackendMessage):
    key = "K"
    struct = Struct("pid" / Int32ub, "key" / Bytes(4))

    def print(message):
        # Both of these should be censored, for reproducible regression test output
        return "BackendKeyData(XXX)"


class NoticeResponse(BackendMessage):
    key = "N"
    struct = Struct(
        "notices"
        / GreedyRange(
            Struct(
                "key"
                / Enum(
                    Byte,
                    severity=ord("S"),
                    _severity_not_localized=ord("V"),
                    _sql_state=ord("C"),
                    message=ord("M"),
                    detail=ord("D"),
                    hint=ord("H"),
                    _position=ord("P"),
                    _internal_position=ord("p"),
                    _internal_query=ord("q"),
                    _where=ord("W"),
                    schema_name=ord("s"),
                    table_name=ord("t"),
                    column_name=ord("c"),
                    data_type_name=ord("d"),
                    constraint_name=ord("n"),
                    _file_name=ord("F"),
                    _line_no=ord("L"),
                    _routine_name=ord("R"),
                ),
                "value" / CString("ASCII"),
            )
        )
    )

    def print(message):
        return "NoticeResponse({})".format(
            ", ".join(
                "{}={}".format(response.key, response.value)
                for response in message.notices
                if not response.key.startswith("_")
            )
        )


class Parse(FrontendMessage):
    key = "P"
    struct = Struct(
        "name" / CString("ASCII"),
        "query" / CString("ASCII"),
        "_parametercount" / Int16ub,
        "parameters" / Array(this._parametercount, Int32ub),
    )


class ParseComplete(BackendMessage):
    key = "1"
    struct = Struct()


class Bind(FrontendMessage):
    key = "B"
    struct = Struct(
        "destination_portal" / CString("ASCII"),
        "prepared_statement" / CString("ASCII"),
        "_parameter_format_code_count" / Int16ub,
        "parameter_format_codes" / Array(this._parameter_format_code_count, Int16ub),
        "_parameter_value_count" / Int16ub,
        "parameter_values"
        / Array(
            this._parameter_value_count,
            Struct("length" / Int32ub, "value" / Bytes(this.length)),
        ),
        "result_column_format_count" / Int16ub,
        "result_column_format_codes" / Array(this.result_column_format_count, Int16ub),
    )


class BindComplete(BackendMessage):
    key = "2"
    struct = Struct()


class NoData(BackendMessage):
    key = "n"
    struct = Struct()


class Describe(FrontendMessage):
    key = "D"
    struct = Struct(
        "type" / Enum(Byte, prepared_statement=ord("S"), portal=ord("P")),
        "name" / CString("ASCII"),
    )

    def print(message):
        return "Describe({}={})".format(message.type, message.name or "<unnamed>")


class Execute(FrontendMessage):
    key = "E"
    struct = Struct("name" / CString("ASCII"), "max_rows_to_return" / Int32ub)

    def print(message):
        return "Execute({}, max_rows_to_return={})".format(
            message.name or "<unnamed>", message.max_rows_to_return
        )


class Sync(FrontendMessage):
    key = "S"
    struct = Struct()


frontend_switch = Switch(
    this.type,
    {**FrontendMessage.name_to_struct(), **SharedMessage.name_to_struct()},
    default=Bytes(this.length - 4),
)

backend_switch = Switch(
    this.type,
    {**BackendMessage.name_to_struct(), **SharedMessage.name_to_struct()},
    default=Bytes(this.length - 4),
)

frontend_msgtypes = Enum(
    Byte, **{**FrontendMessage.name_to_key(), **SharedMessage.name_to_key()}
)

backend_msgtypes = Enum(
    Byte, **{**BackendMessage.name_to_key(), **SharedMessage.name_to_key()}
)

# It might seem a little circuitous to say a frontend message is a kind of frontend
# message but this lets us easily customize how they're printed


class Frontend(FrontendMessage):
    struct = Struct(
        "type" / frontend_msgtypes,
        "length" / Int32ub,  # "32-bit unsigned big-endian"
        "raw_body" / Bytes(this.length - 4),
        # try to parse the body into something more structured than raw bytes
        "body" / RestreamData(this.raw_body, frontend_switch),
    )

    def print(message):
        if isinstance(message.body, bytes):
            return "Frontend(type={},body={})".format(chr(message.type), message.body)
        return FrontendMessage.print_message(message.body)

    def typeof(message):
        if isinstance(message.body, bytes):
            return "Unknown"
        return message.body._type


class Backend(BackendMessage):
    struct = Struct(
        "type" / backend_msgtypes,
        "length" / Int32ub,  # "32-bit unsigned big-endian"
        "raw_body" / Bytes(this.length - 4),
        # try to parse the body into something more structured than raw bytes
        "body" / RestreamData(this.raw_body, backend_switch),
    )

    def print(message):
        if isinstance(message.body, bytes):
            return "Backend(type={},body={})".format(chr(message.type), message.body)
        return BackendMessage.print_message(message.body)

    def typeof(message):
        if isinstance(message.body, bytes):
            return "Unknown"
        return message.body._type


# GreedyRange keeps reading messages until we hit EOF
frontend_messages = GreedyRange(Frontend.struct)
backend_messages = GreedyRange(Backend.struct)


def parse(message, from_frontend=True):
    if from_frontend:
        message = frontend_messages.parse(message)
    else:
        message = backend_messages.parse(message)
    message.from_frontend = from_frontend

    return message


def print(message):
    if message.from_frontend:
        return FrontendMessage.print_message(message)
    return BackendMessage.print_message(message)


def message_type(message, from_frontend):
    if from_frontend:
        return FrontendMessage.find_typeof(message)
    return BackendMessage.find_typeof(message)


def message_matches(message, filters, from_frontend):
    """
    Message is something like Backend(Query)) and fiters is something like query="COPY".

    For now we only support strings, and treat them like a regex, which is matched against
    the content of the wrapped message
    """
    if message._type != "Backend" and message._type != "Frontend":
        raise ValueError("can't handle {}".format(message._type))

    wrapped = message.body
    if isinstance(wrapped, bytes):
        # we don't know which kind of message this is, so we can't match against it
        return False

    for key, value in filters.items():
        if not isinstance(value, str):
            raise ValueError("don't yet know how to handle {}".format(type(value)))

        actual = getattr(wrapped, key)

        if not re.search(value, actual):
            return False

    return True
