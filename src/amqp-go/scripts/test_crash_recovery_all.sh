#!/bin/bash

# Comprehensive Crash Recovery Stress Test
# Tests WAL recovery at multiple scales: 100, 1K, 10K, 100K messages
# Usage: ./scripts/test_crash_recovery_all.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_LEVELS=(100 1000 10000 100000)

echo "=================================================="
echo "AMQP Crash Recovery Stress Test Suite"
echo "=================================================="
echo "Testing recovery at 4 scales: ${TEST_LEVELS[@]}"
echo ""

# Results tracking
RESULTS=()
FAILED=0

# Run each test level
for COUNT in "${TEST_LEVELS[@]}"; do
    echo ""
    echo "=================================================="
    echo "Testing with $COUNT messages"
    echo "=================================================="

    START=$(date +%s)

    if "$SCRIPT_DIR/crash_test.sh" "$COUNT"; then
        END=$(date +%s)
        DURATION=$((END - START))
        RESULTS+=("✓ $COUNT messages: PASSED (${DURATION}s)")
    else
        END=$(date +%s)
        DURATION=$((END - START))
        RESULTS+=("✗ $COUNT messages: FAILED (${DURATION}s)")
        FAILED=$((FAILED + 1))
    fi

    # Brief pause between tests
    sleep 2
done

# Summary report
echo ""
echo "=================================================="
echo "CRASH RECOVERY TEST SUITE SUMMARY"
echo "=================================================="
for RESULT in "${RESULTS[@]}"; do
    echo "$RESULT"
done

echo ""
if [ $FAILED -eq 0 ]; then
    echo "=================================================="
    echo "✓ ALL TESTS PASSED"
    echo "=================================================="
    echo "WAL crash recovery working correctly at all scales!"
    echo "At-least-once delivery semantics verified."
    exit 0
else
    echo "=================================================="
    echo "✗ $FAILED TEST(S) FAILED"
    echo "=================================================="
    echo "Check logs in /tmp/amqp-crash-test*.log"
    exit 1
fi
