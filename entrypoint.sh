#!/bin/bash
set -e

# Get delay from environment variable (default to 10 seconds if not set)
DELAY=${DELAY:-10}
PIPELINE=${PIPELINE:-""}
EXCHANGE_FILTER=${EXCHANGE_FILTER:-""}

echo "=========================================="
echo "🚀 Starting Coinglass Pipeline Runner"
echo "=========================================="
echo "Pipeline: ${PIPELINE}"
echo "Exchange Filter: ${EXCHANGE_FILTER}"
echo "Delay: ${DELAY} seconds"
echo "=========================================="

# Run the pipeline in a loop with the specified delay
while true; do
    echo ""
    echo "⏰ $(date '+%Y-%m-%d %H:%M:%S') - Running ${PIPELINE}${EXCHANGE_FILTER:+ for $EXCHANGE_FILTER}"

    # Run the pipeline
    python main.py "${PIPELINE}"

    EXIT_CODE=$?

    if [ $EXIT_CODE -ne 0 ]; then
        echo "⚠️  Pipeline exited with code $EXIT_CODE"
    else
        echo "✅ Pipeline completed successfully"
    fi

    echo "💤 Sleeping for ${DELAY} seconds..."
    sleep "${DELAY}"
done
