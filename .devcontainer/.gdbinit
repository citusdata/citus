# gdbpg.py contains scripts to nicely print the postgres datastructures
# while in a gdb session. Since the vscode debugger is based on gdb this
# actually also works when debugging with vscode. Providing nice tools
# to understand the internal datastructures we are working with.
source /root/gdbpg.py

# when debugging postgres it is convenient to _always_ have a breakpoint
# trigger when an error is logged. Because .gdbinit is sourced before gdb
# is fully attached and has the sources loaded. To make sure the breakpoint
# is added when the library is loaded we temporary set the breakpoint pending
# to on. After we have added out breakpoint we revert back to the default
# configuration for breakpoint pending.
# The breakpoint is hard to read, but at entry of the function we don't have
# the level loaded in elevel. Instead we hardcode the location where the
# level of the current error is stored. Also gdb doesn't understand the
# ERROR symbol so we hardcode this to the value of ERROR. It is very unlikely
# this value will ever change in postgres, but if it does we might need to
# find a way to conditionally load the correct breakpoint.
# Lastly the breakpoint doesn't show up in vscode :(, to remove the breakpoint
# you can use the command `-exec delete <id>` in the vscode debug console. The
# id is the number in the first column of the breakpoint list which you can
# show with the command `-exec info break`.
set breakpoint pending on
break elog.c:errfinish if errordata[errordata_stack_depth].elevel == 21
set breakpoint pending auto
