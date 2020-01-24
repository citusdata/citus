# Rules to normalize test outputs.
#
# This file contains all the rules that completely remove lines. These lines
# are removed because they don't appear consistently.

# ignore could not consume warnings
/WARNING:  could not consume data from worker node/d

# ignore WAL warnings
/DEBUG: .+creating and filling new WAL file/d

# Line info varies between versions
/^LINE [0-9]+:.*$/d
/^ *\^$/d
