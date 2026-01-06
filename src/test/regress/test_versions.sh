#!/bin/bash

OUTPUT_FILE="version_test_results.txt"
RESULTS_FILE="results/versions.out"

# Clear previous results
> "$OUTPUT_FILE"

echo "Starting version tests at $(date)" | tee -a "$OUTPUT_FILE"
echo "======================================" | tee -a "$OUTPUT_FILE"
echo "" | tee -a "$OUTPUT_FILE"

run_test() {
    local cmd="$1"
    echo "Running: $cmd" | tee -a "$OUTPUT_FILE"
    echo "--------------------------------------" | tee -a "$OUTPUT_FILE"
    
    # Run the command and capture exit code, but don't exit on failure
    set +e
    eval "$cmd"
    set -e
    
    # Always capture the output if it exists
    if [ -f "$RESULTS_FILE" ]; then
        echo "" >> "$OUTPUT_FILE"
        cat "$RESULTS_FILE" >> "$OUTPUT_FILE"
    else
        echo "WARNING: $RESULTS_FILE not found" | tee -a "$OUTPUT_FILE"
    fi
    
    echo "" | tee -a "$OUTPUT_FILE"
    echo "======================================" | tee -a "$OUTPUT_FILE"
    echo "" | tee -a "$OUTPUT_FILE"
}

#Default 
run_test "EXTRA_TESTS=versions make check-minimal"

# Test 1: Version-only tests
run_test "CITUSVERSION=13.2-1 N1MODE=workeronly EXTRA_TESTS=versions make check-minimal"
run_test "CITUSVERSION=13.2-1 N1MODE=coordinatoronly EXTRA_TESTS=versions make check-minimal"
run_test "CITUSVERSION=13.2-1 N1MODE=all EXTRA_TESTS=versions make check-minimal"

# Test 2: Libdir-only tests
run_test "CITUSLIBDIR=~/citus-libs/17/v13.2.0 N1MODE=workeronly EXTRA_TESTS=versions make check-minimal"
run_test "CITUSLIBDIR=~/citus-libs/17/v13.2.0 N1MODE=coordinatoronly EXTRA_TESTS=versions make check-minimal"
run_test "CITUSLIBDIR=~/citus-libs/17/v13.2.0 N1MODE=all EXTRA_TESTS=versions make check-minimal"

# Test 3: Combined tests
run_test "CITUSLIBDIR=~/citus-libs/17/v13.2.0 CITUSVERSION=13.2-1 N1MODE=workeronly EXTRA_TESTS=versions make check-minimal"
run_test "CITUSLIBDIR=~/citus-libs/17/v13.2.0 CITUSVERSION=13.2-1 N1MODE=coordinatoronly EXTRA_TESTS=versions make check-minimal"
run_test "CITUSLIBDIR=~/citus-libs/17/v13.2.0 CITUSVERSION=13.2-1 N1MODE=all EXTRA_TESTS=versions make check-minimal"

echo "All tests completed at $(date)" | tee -a "$OUTPUT_FILE"
echo "Results saved to: $OUTPUT_FILE"
