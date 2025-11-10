#!/bin/bash

# Quick rebuild script to fix entrypoint.sh issue
# This script stops all containers, removes old images, and rebuilds

set -e

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}🔄 Quick Rebuild Script${NC}"
echo -e "${YELLOW}This will stop all services, remove old images, and rebuild${NC}"
echo ""

# Step 1: Stop all services
echo -e "${BLUE}Step 1/5: Stopping all services...${NC}"
docker-compose --profile derivatives --profile exchange --profile spot --profile etf --profile trading down --remove-orphans

# Step 2: Remove old images
echo -e "${BLUE}Step 2/5: Removing old images...${NC}"
docker-compose down --rmi all --remove-orphans || true

# Step 3: Ensure entrypoint.sh is executable
echo -e "${BLUE}Step 3/5: Setting entrypoint.sh permissions...${NC}"
chmod +x entrypoint.sh

# Step 4: Rebuild with no cache
echo -e "${BLUE}Step 4/5: Building fresh images (no cache)...${NC}"
docker-compose build --no-cache

# Step 5: Deploy
echo -e "${BLUE}Step 5/5: Starting services...${NC}"
read -p "Which profile to deploy? (all/derivatives/exchange/spot/etf/trading): " PROFILE

case "$PROFILE" in
    all)
        docker-compose --profile derivatives --profile exchange --profile spot --profile etf --profile trading up -d
        ;;
    derivatives|exchange|spot|etf|trading)
        docker-compose --profile "$PROFILE" up -d
        ;;
    *)
        echo -e "${RED}Invalid profile. Starting with production profile...${NC}"
        docker-compose --profile production up -d
        ;;
esac

echo ""
echo -e "${GREEN}✅ Rebuild completed!${NC}"
echo ""
echo "Check status with: docker-compose ps"
echo "View logs with: docker-compose logs -f"
