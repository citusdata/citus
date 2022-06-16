# Commands

The commands module is modeled after `backend/commands` from the postgres repository and
contains the logic for Citus on how to run these commands on distributed objects. Even
though the structure of the directory has some resemblence to its postgres relative, files
here are somewhat more fine-grained. This is due to the nature of citus commands that are
heavily focused on distributed tables. Instead of having all commands in `tablecmds.c`
they are often moved to files that are named after the command.

| File                         | Description |
|------------------------------|-------------|
| `create_distributed_table.c` | Implementation of UDF's for creating distributed tables |
| `drop_distributed_table.c`   | Implementation for dropping metadata for partitions of distributed tables |
| `extension.c`                | Implementation of `CREATE EXTENSION` commands for citus specific checks |
| `foreign_constraint.c`       | Implementation of and helper functions for foreign key constraints |
| `grant.c`                    | Implementation of `GRANT` commands for roles/users on relations |
| `index.c`                    | Implementation of commands specific to indices on distributed tables |
| `multi_copy.c`               | Implementation of `COPY` command. There are multiple different copy modes which are described in detail below |
| `policy.c`                   | Implementation of `CREATE\ALTER POLICY` commands. |
| `rename.c`                   | Implementation of `ALTER ... RENAME ...` commands. It implements the renaming of applicable objects, otherwise provides the user with a warning |
| `schema.c`                   | |
| `sequence.c`                 | Implementation of `CREATE/ALTER SEQUENCE` commands. Primarily checks correctness of sequence statements as they are not propagated to the worker nodes |
| `table.c`                    | |
| `transmit.c`                 | Implementation of `COPY` commands with `format transmit` set in the options. This format is used to transfer files from one node to another node |
| `truncate.c`                 | Implementation of `TRUNCATE` commands on distributed tables |
| `utility_hook.c`             | This is the entry point from postgres into the commands module of citus. It contains the implementation that gets registered in postgres' `ProcessUtility_hook` callback to extends the functionality of the original ProcessUtility. This code is used to route the incoming commands to their respective implementation in Citus |
| `vacuum.c`                   | Implementation of `VACUUM` commands on distributed tables |

# COPY

The copy command is overloaded for a couple of use-cases specific to citus. The syntax of
the command stays the same, however the implementation might slightly differ from the
stock implementation. The overloading is mostly done via extra options that Citus uses to
indicate how to operate the copy. The options used are described below.

## FORMAT transmit

Implemented in `transmit.c`

TODO: to be written by someone with enough knowledge to write how, when and why it is used.

## FORMAT result

Implemented in `multi_copy.c`

TODO: to be written by someone with enough knowledge to write how, when and why it is used.
