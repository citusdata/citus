# https://stackoverflow.com/a/51808896/2570866
function read(file,indent) {
    if ( isOpen[file]++ ) {
        print "Infinite recursion detected" | "cat>&2"
        exit 1
    }

    while ( (getline < file) > 0) {
        if ($1 == "@include") {
            match($0,/^[[:space:]]+/)
            read($2,indent substr($0,1,RLENGTH))
        } else {
            print indent $0
        }
    }
    close(file)

    delete isOpen[file]
}

BEGIN{
   read(ARGV[1],"")
   exit
}
