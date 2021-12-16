#! /bin/bash

set -euo pipefail

# shellcheck disable=SC1091
source ci/ci_helpers.sh

# We do not use comments starting with # in spec files because it creates warnings from
# preprocessor that expects directives after this character.

# `# ` -> `-- `
find src/test/regress/spec/*.spec -print0 | xargs -0 sed -i 's!# !// !g'
