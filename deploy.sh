#!/bin/bash

# Deploy script for automation_coinglass on Docker server
# This script will setup the Docker environment and deploy the application

set -e

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Starting deployment process...${NC}"

# Function to display usage
show_usage() {
    echo "Usage: ./deploy.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --all-profiles     Deploy all services (derivatives, exchange, spot, etf, trading, cryptoquant)"
    echo "  --derivatives      Deploy only derivatives pipelines"
    echo "  --exchange         Deploy only exchange pipelines"
    echo "  --spot             Deploy only spot market pipelines"
    echo "  --etf              Deploy only Bitcoin ETF pipelines"
    echo "  --trading          Deploy only trading market pipelines"
    echo "  --cryptoquant      Deploy only CryptoQuant pipelines"
    echo "  --production       Deploy all production services"
    echo "  --setup            Run database setup only"
    echo "  --no-build         Skip Docker image rebuild"
    echo "  --help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./deploy.sh                    # Deploy with default settings"
    echo "  ./deploy.sh --all-profiles     # Deploy all services at once"
    echo "  ./deploy.sh --derivatives      # Deploy only derivatives services"
    echo "  ./deploy.sh --setup            # Run database setup"
    echo ""
}

# Check if Docker and Docker Compose are installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ Docker Compose is not installed. Please install Docker Compose first.${NC}"
    exit 1
fi

# Get the directory of the script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo -e "${BLUE}📁 Working directory: $SCRIPT_DIR${NC}"

# Parse command line arguments
PROFILES=""
SKIP_BUILD=false

case "${1:-}" in
    --all-profiles)
        PROFILES="--profile derivatives --profile exchange --profile spot --profile etf --profile trading --profile cryptoquant"
        echo -e "${GREEN}🎯 Deploying ALL profiles${NC}"
        ;;
    --cryptoquant)
        PROFILES="--profile cryptoquant"
        echo -e "${GREEN}🎯 Deploying cryptoquant profile${NC}"
        ;;
    --derivatives)
        PROFILES="--profile derivatives"
        echo -e "${GREEN}🎯 Deploying derivatives profile${NC}"
        ;;
    --exchange)
        PROFILES="--profile exchange"
        echo -e "${GREEN}🎯 Deploying exchange profile${NC}"
        ;;
    --spot)
        PROFILES="--profile spot"
        echo -e "${GREEN}🎯 Deploying spot profile${NC}"
        ;;
    --etf)
        PROFILES="--profile etf"
        echo -e "${GREEN}🎯 Deploying etf profile${NC}"
        ;;
    --trading)
        PROFILES="--profile trading"
        echo -e "${GREEN}🎯 Deploying trading profile${NC}"
        ;;
    --production)
        PROFILES="--profile production"
        echo -e "${GREEN}🎯 Deploying production profile${NC}"
        ;;
    --setup)
        echo -e "${GREEN}🎯 Running database setup${NC}"
        docker-compose build
        docker-compose --profile setup up
        echo -e "${GREEN}✅ Setup completed${NC}"
        exit 0
        ;;
    --no-build)
        SKIP_BUILD=true
        echo -e "${YELLOW}⚠️  Skipping Docker build${NC}"
        ;;
    --help)
        show_usage
        exit 0
        ;;
    "")
        # Default: no specific profile
        echo -e "${GREEN}🎯 Deploying default configuration${NC}"
        ;;
    *)
        echo -e "${RED}❌ Unknown option: $1${NC}"
        echo ""
        show_usage
        exit 1
        ;;
esac

# Verify entrypoint.sh exists BEFORE building
if [ ! -f "entrypoint.sh" ]; then
    echo -e "${RED}❌ entrypoint.sh not found! Please ensure it exists in the project root.${NC}"
    exit 1
fi

# Create necessary directories if they don't exist
echo -e "${BLUE}📂 Creating necessary directories...${NC}"
mkdir -p logs data

# Set proper permissions
echo -e "${BLUE}🔐 Setting permissions...${NC}"
chmod +x entrypoint.sh 2>/dev/null || true
chmod +x scripts/*.sh 2>/dev/null || true
chmod +x stop.sh 2>/dev/null || true

# Stop and remove existing containers (if any)
echo -e "${YELLOW}🛑 Stopping existing containers...${NC}"
docker-compose down --remove-orphans || true

# Build the Docker image
if [ "$SKIP_BUILD" = false ]; then
    echo -e "${BLUE}🔨 Building Docker image...${NC}"
    docker-compose build --no-cache
else
    echo -e "${YELLOW}⏭️  Skipping build step${NC}"
fi

# Start the services
echo -e "${GREEN}▶️  Starting services...${NC}"
if [ -n "$PROFILES" ]; then
    docker-compose $PROFILES up -d
else
    docker-compose up -d
fi

# Wait for services to be ready
echo -e "${YELLOW}⏳ Waiting for services to be ready...${NC}"
sleep 10

# Check if services are running
echo -e "${GREEN}✅ Checking service status...${NC}"
docker-compose ps

# Show logs
echo -e "${BLUE}📋 Showing recent logs...${NC}"
docker-compose logs --tail=20

echo ""
echo -e "${GREEN}🎉 Deployment completed successfully!${NC}"
echo ""
echo -e "${BLUE}📊 Quick Commands:${NC}"
echo ""
echo -e "${YELLOW}Deploy:${NC}"
echo "   ./deploy.sh --all-profiles   # Deploy all services at once"
echo "   ./deploy.sh --derivatives    # Deploy only derivatives"
echo "   ./deploy.sh --exchange       # Deploy only exchange"
echo "   ./deploy.sh --spot           # Deploy only spot"
echo "   ./deploy.sh --etf            # Deploy only ETF"
echo "   ./deploy.sh --trading        # Deploy only trading"
    echo "   ./deploy.sh --cryptoquant    # Deploy only CryptoQuant"
echo ""
echo -e "${YELLOW}Stop:${NC}"
echo "   ./stop.sh                    # Stop all services"
echo "   ./stop.sh --derivatives      # Stop only derivatives"
echo "   ./stop.sh --service <name>   # Stop specific service"
echo ""
echo -e "${YELLOW}Monitor:${NC}"
echo "   docker-compose ps            # Check service status"
echo "   docker-compose logs -f       # View all logs in real-time"
echo "   docker-compose logs -f <service-name>  # View specific service logs"
echo ""
echo -e "${YELLOW}Manage:${NC}"
echo "   docker-compose restart [service-name]  # Restart services"
echo "   docker-compose exec <service-name> bash  # Access container shell"
echo ""
echo -e "${BLUE}💡 For more options: ./deploy.sh --help  or  ./stop.sh --help${NC}"
echo ""