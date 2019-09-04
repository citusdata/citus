# modified version of: https://stackoverflow.com/a/51808896/2570866
# Changes:
# - Added check if file could actually be read
function read(file,indent) {
    if ( isOpen[file]++ ) {
        print "Infinite recursion detected" | "cat>&2"
        exit 1
    }

    lines = 0

    while ( (getline < file) > 0) {
        lines++
        if ($1 == "@include") {
            match($0,/^[[:space:]]+/)
            read($2,indent substr($0,1,RLENGTH))
        } else {
            print indent $0
        }
    }
    close(file)

    if (lines == 0) {
        print file ": could not be read (possibly because it does not exist)" > "/dev/stderr"
        exit 1
    }

    delete isOpen[file]
}

BEGIN{
   read(ARGV[1],"")
   exit
}
