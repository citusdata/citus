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
set breakpoint pending on
break elog.c:errfinish if errordata[errordata_stack_depth].elevel == 21
set breakpoint pending auto

echo \n
echo ----------------------------------------------------------------------------------\n
echo when attaching to a postgres backend a breakpoint will be set on elog.c:errfinish \n
echo it will only break on errors being raised in postgres \n
echo \n
echo to disable this breakpoint from vscode run `-exec disable 1` in the debug console \n
echo this assumes it's the first breakpoint loaded as it is loaded from .gdbinit \n
echo this can be verified with `-exec info break`, enabling can be done with \n
echo `-exec enable 1` \n
echo ----------------------------------------------------------------------------------\n
echo \n
