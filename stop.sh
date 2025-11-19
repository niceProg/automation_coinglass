#!/bin/bash

# Stop script for automation_coinglass Docker services
# This script provides various options to stop running containers

set -e

# Get the directory of the script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🛑 Coinglass Automation - Stop Service${NC}"
echo ""

# Function to display usage
show_usage() {
    echo "Usage: ./stop.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --all              Stop all services across ALL profiles (default)"
    echo "  --derivatives      Stop only derivatives pipelines (funding_rate, open_interest, etc.)"
    echo "  --exchange         Stop only exchange pipelines"
    echo "  --spot             Stop only spot market pipelines"
    echo "  --etf              Stop only Bitcoin ETF pipelines"
    echo "  --trading          Stop only trading market pipelines"
    echo "  --cryptoquant      Stop only CryptoQuant pipelines"
    echo "  --sentiment        Stop only sentiment pipelines (fear_greed_index, hyperliquid_whale_alert, whale_transfer)"
    echo "  --macro            Stop only macro pipelines (bitcoin_vs_global_m2_growth)"
    echo "  --production       Stop all production services"
    echo "  --monitoring       Stop monitoring services (status, freshness)"
    echo "  --endpoints        Stop specific endpoints: hyperliquid_whale_alert, bitcoin_vs_global_m2_growth, whale_transfer, fear_greed_index"
    echo "  --service <name>   Stop a specific service by name"
    echo "  --remove           Stop and remove containers, networks, and volumes (ALL profiles)"
    echo "  --help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./stop.sh                          # Stop all services (derivatives, exchange, spot, etf, trading, cryptoquant)"
    echo "  ./stop.sh --derivatives            # Stop only derivatives services"
    echo "  ./stop.sh --sentiment              # Stop only sentiment services"
    echo "  ./stop.sh --macro                  # Stop only macro services"
    echo "  ./stop.sh --endpoints              # Stop specific endpoints (hyperliquid_whale_alert, bitcoin_vs_global_m2_growth, whale_transfer, fear_greed_index)"
    echo "  ./stop.sh --service funding_rate   # Stop only funding_rate service"
    echo "  ./stop.sh --remove                 # Stop and remove everything"
    echo ""
}

# Function to stop all services across all profiles
stop_all() {
    echo -e "${YELLOW}🛑 Stopping all services across all profiles...${NC}"
    docker-compose --profile derivatives --profile exchange --profile spot --profile etf --profile trading --profile cryptoquant --profile sentiment --profile macro --profile monitoring --profile setup down
    echo -e "${GREEN}✅ All services stopped${NC}"
}

# Function to stop services by profile
stop_profile() {
    local profile=$1
    echo -e "${YELLOW}🛑 Stopping $profile services...${NC}"
    docker-compose --profile "$profile" down
    echo -e "${GREEN}✅ $profile services stopped${NC}"
}

# Function to stop a specific service
stop_service() {
    local service=$1
    echo -e "${YELLOW}🛑 Stopping service: $service...${NC}"
    docker-compose stop "$service"
    docker-compose rm -f "$service"
    echo -e "${GREEN}✅ Service $service stopped${NC}"
}

# Function to stop the specific requested endpoints
stop_requested_endpoints() {
    echo -e "${YELLOW}🛑 Stopping specific endpoints: hyperliquid_whale_alert, bitcoin_vs_global_m2_growth, whale_transfer, fear_greed_index...${NC}"
    docker-compose stop hyperliquid_whale_alert bitcoin_vs_global_m2_growth whale_transfer fear_greed_index
    docker-compose rm -f hyperliquid_whale_alert bitcoin_vs_global_m2_growth whale_transfer fear_greed_index
    echo -e "${GREEN}✅ Requested endpoints stopped${NC}"
}

# Function to stop and remove everything
stop_and_remove() {
    echo -e "${RED}🗑️  Stopping and removing all containers, networks, and volumes...${NC}"
    echo -e "${YELLOW}⚠️  This will remove all data. Press Ctrl+C to cancel...${NC}"
    sleep 3
    docker-compose --profile derivatives --profile exchange --profile spot --profile etf --profile trading --profile cryptoquant --profile sentiment --profile macro --profile monitoring --profile setup down --volumes --remove-orphans
    echo -e "${GREEN}✅ All containers, networks, and volumes removed${NC}"
}

# Function to show container status
show_status() {
    echo ""
    echo -e "${BLUE}📊 Current container status:${NC}"
    docker-compose ps
    echo ""
}

# Parse command line arguments
case "${1:-}" in
    --all)
        stop_all
        show_status
        ;;
    --derivatives)
        stop_profile "derivatives"
        show_status
        ;;
    --exchange)
        stop_profile "exchange"
        show_status
        ;;
    --spot)
        stop_profile "spot"
        show_status
        ;;
    --etf)
        stop_profile "etf"
        show_status
        ;;
    --trading)
        stop_profile "trading"
        show_status
        ;;
    --cryptoquant)
        stop_profile "cryptoquant"
        show_status
        ;;
    --sentiment)
        stop_profile "sentiment"
        show_status
        ;;
    --macro)
        stop_profile "macro"
        show_status
        ;;
    --endpoints)
        stop_requested_endpoints
        show_status
        ;;
    --production)
        stop_profile "production"
        show_status
        ;;
    --monitoring)
        stop_profile "monitoring"
        show_status
        ;;
    --service)
        if [ -z "${2:-}" ]; then
            echo -e "${RED}❌ Error: Service name required${NC}"
            echo "Usage: ./stop.sh --service <service_name>"
            exit 1
        fi
        stop_service "$2"
        show_status
        ;;
    --remove)
        stop_and_remove
        show_status
        ;;
    --help)
        show_usage
        ;;
    "")
        # Default: stop all
        stop_all
        show_status
        ;;
    *)
        echo -e "${RED}❌ Unknown option: $1${NC}"
        echo ""
        show_usage
        exit 1
        ;;
esac

echo ""
echo -e "${GREEN}🎉 Stop operation completed!${NC}"
echo ""
echo -e "${BLUE}💡 Quick commands:${NC}"
echo "  View logs:        docker-compose logs -f [service_name]"
echo "  Start services:   ./deploy.sh"
echo "  Check status:     docker-compose ps"
echo ""
