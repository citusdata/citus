#!/bin/bash
#
# autogen.sh converts configure.in to configure and creates
# citus_config.h.in. The resuting resulting files are checked into
# the SCM, to avoid everyone needing autoconf installed.

autoreconf -f
