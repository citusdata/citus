#!/usr/bin/env bash

cmd="$@"

$cmd &
regress_pid=$!

# the regression tests don't take 60 seconds to run, we can time out if they do
sleep 60 &
sleep_pid=$!

wait -n
process_status=$?

if $(kill -0 $regress_pid); then
  echo "it's been 60 seconds and regress is still running"

  printf 'is mitmdump running?\n'
  ps aux | grep mitm

  # what is mitmdump doing?
  sudo timeout 1 strace -p `pgrep mitmdump`

  # okay, it's time to finish
  kill -SIGINT $regress_pid

  # the tests have timed out, this is a failure!
  exit 1
else
  # there's a race condition here but I find it unlikely the tests will take exactly
  # 60 seconds to run
  echo "regress has successfully finished"
  # exit with the same error code regress finished with
  exit $process_status
fi
