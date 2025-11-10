#!/bin/bash
echo "🧹 Clearing all Python cache..."

# Remove all __pycache__ directories
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null

# Remove all .pyc files
find . -name "*.pyc" -delete 2>/dev/null

# Remove all .pyo files
find . -name "*.pyo" -delete 2>/dev/null

# Remove pytest cache
find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null

# Remove any .egg-info directories
find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null

echo "✅ Cache cleared successfully!"
echo ""
echo "Now run: python main.py spot_orderbook_aggregated"
