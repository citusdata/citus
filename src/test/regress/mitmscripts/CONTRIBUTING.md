Contributing
============

For each message we wish to capture, we have a class definition in `structs.py`.

If there is a new network message that is not yet parsed by our proxy, check the Postgres documentation [here](https://www.postgresql.org/docs/current/protocol-message-formats.html) for message format and add a new class definition.

Rooms for improvement:
- Anonymize network dumps by removing shard/placement/transaction ids
  - Occasionally changes in our codebase introduces new messages that contain parts that should be anonymized
- Add missing message format definitions
- Allow failure testing on underprivileged users are not allowed to write to our fifo file on the database

# Resources at Postgres Docs:

  - [Postgres Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html) is the root directory for message protocols between frontends and backends.
    - [Protocol Flow](https://www.postgresql.org/docs/current/protocol-flow.html) explains the lifecyle of a session, and a tentative ordering of messages that will be dispatched
      - [Extended Query Protocol](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY) uses a more detailed set of messages in the session lifecycle, and these messages are mostly left unparsed.
    - [Message Formats](https://www.postgresql.org/docs/current/protocol-message-formats.html) lists formats of all the messages that can be dispatched
