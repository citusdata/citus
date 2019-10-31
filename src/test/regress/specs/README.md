In this folder, all tests which in the format of '*_add.spec' organized
according to specific format.

You should use `//` in mx files not `#`. We preprocess mx files with `cpp` to
include `isolation_mx_common.spec`.

For isolation tests, we selected 'n' representative operations and we aimed to
perform all possible pairs of 'n' operations together. So first test just runs
first of these 'n' operation with remaining 'n - 1' operation. Similary, second
test just runs second operation with remaining 'n - 2' operation. With this
logic, we eventually run every selected operation with every other selected
operation.
