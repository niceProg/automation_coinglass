# Coinglass Automation - Docker Deployment Guide

## Overview
This Docker configuration runs each Coinglass pipeline as a separate container with 1-second intervals, optimized for high-frequency data collection in production environments.

## Architecture
- **22 Pipeline Containers**: Each pipeline runs independently every 1 second
- **System Services**: Setup, status, and freshness monitoring
- **Network Isolation**: All containers communicate via dedicated Docker network
- **Environment Management**: Centralized configuration via environment variables

## Quick Start

### 1. Environment Setup
```bash
# Copy environment template
cp .env.example .env

# Edit .env with your configuration
nano .env
```

### 2. Initial Setup
```bash
# Build and run database setup
docker-compose --profile setup up --build

# Check setup status
docker-compose --profile monitoring up status
```

### 3. Production Deployment
```bash
# Run ALL pipelines (production mode)
docker-compose --profile production up -d

# Or run specific categories
docker-compose --profile derivatives up -d
docker-compose --profile spot up -d
docker-compose --profile etf up -d
```

## Pipeline Categories

### 🔧 System Administration
```bash
# Setup database tables
docker-compose --profile setup up setup

# Check system status
docker-compose --profile monitoring up status

# Check data freshness
docker-compose --profile monitoring up freshness
```

### 📈 Derivatives Market (5 containers)
```bash
# Run all derivatives pipelines
docker-compose --profile derivatives up -d

# Individual pipelines
docker-compose up -d funding_rate
docker-compose up -d open_interest
docker-compose up -d long_short_ratio
docker-compose up -d liquidation
docker-compose up -d options
```

### 🏦 Exchange Infrastructure (3 containers)
```bash
# Run all exchange pipelines
docker-compose --profile exchange up -d

# Individual pipelines
docker-compose up -d exchange_assets
docker-compose up -d exchange_balance_list
docker-compose up -d exchange_onchain_transfers
```

### 💰 Spot Market (6 containers)
```bash
# Run all spot market pipelines
docker-compose --profile spot up -d

# Individual pipelines
docker-compose up -d spot_supported_exchange_pairs
docker-compose up -d spot_coins_markets
docker-compose up -d spot_pairs_markets
docker-compose up -d spot_price_history
```

### ₿ Bitcoin ETF (4 containers)
```bash
# Run all Bitcoin ETF pipelines
docker-compose --profile etf up -d

# Individual pipelines
docker-compose up -d bitcoin_etf_list
docker-compose up -d bitcoin_etf_history
docker-compose up -d bitcoin_etf_flows_history
docker-compose up -d bitcoin_etf_premium_discount_history
```

### 📊 Trading Market (3 containers)
```bash
# Run all trading market pipelines
docker-compose --profile trading up -d

# Individual pipelines
docker-compose up -d supported_exchange_pairs
docker-compose up -d pairs_markets
docker-compose up -d coins_markets
```

## Environment Configuration

### Required Variables
```bash
DB_HOST=localhost          # Database host
DB_PORT=3306              # Database port
DB_USER=your_db_user      # Database username
DB_PASSWORD=your_password # Database password
DB_NAME=coinglass_automation # Database name
COINGLASS_API_KEY=your_key # Coinglass API key
```

### Optional Variables
```bash
COINGLASS_EXCHANGES=binance,bybit,okx,huobi  # Exchanges to monitor
COINGLASS_SYMBOLS=BTC,ETH                      # Symbols to track
MIN_USD=1000000                                # Minimum USD threshold
```

## Container Management

### View Running Containers
```bash
# List all running containers
docker-compose ps

# View specific category containers
docker-compose --profile derivatives ps
```

### Monitor Logs
```bash
# View all logs
docker-compose logs -f

# View specific pipeline logs
docker-compose logs -f funding_rate

# View all derivatives logs
docker-compose --profile derivatives logs -f
```

### Stop Containers
```bash
# Stop all containers
docker-compose down

# Stop specific category
docker-compose --profile derivatives down

# Stop specific container
docker-compose stop funding_rate
```

### Restart Containers
```bash
# Restart all containers
docker-compose restart

# Restart specific category
docker-compose --profile derivatives restart

# Restart specific container
docker-compose restart funding_rate
```

## Production Deployment

### Full Production Setup
```bash
# 1. Setup environment
cp .env.example .env
# Edit .env with your credentials

# 2. Initial database setup
docker-compose --profile setup up --build

# 3. Deploy all pipelines
docker-compose --profile production up -d

# 4. Monitor deployment
docker-compose ps
docker-compose logs -f
```

### Selective Production Deployment
```bash
# Deploy only high-priority pipelines
docker-compose --profile derivatives up -d
docker-compose --profile etf up -d
docker-compose --profile exchange up -d

# Deploy market data pipelines
docker-compose --profile spot up -d
docker-compose --profile trading up -d
```

## Monitoring and Maintenance

### Health Checks
```bash
# Check container health
docker-compose ps

# View health status
docker inspect coinglass_funding_rate | grep Health -A 5
```

### Performance Monitoring
```bash
# View resource usage
docker stats

# View specific container stats
docker stats coinglass_funding_rate
```

### Data Freshness Monitoring
```bash
# Run freshness check
docker-compose --profile monitoring up freshness

# Set up periodic freshness checks (cron)
# Add to crontab:
# */5 * * * * cd /path/to/project && docker-compose --profile monitoring up freshness
```

## Troubleshooting

### Common Issues

1. **Container Won't Start**
   ```bash
   # Check logs
   docker-compose logs funding_rate

   # Check environment variables
   docker-compose exec funding_rate env | grep COINGLASS
   ```

2. **Database Connection Issues**
   ```bash
   # Test database connectivity
   docker-compose exec funding_rate python -c "
   import mysql.connector
   print('Database connection test...')
   "
   ```

3. **API Rate Limiting**
   ```bash
   # Check API key validity
   docker-compose exec funding_rate python -c "
   import os
   print('API Key:', os.getenv('COINGLASS_API_KEY'))
   "
   ```

### Cleanup
```bash
# Remove all containers and networks
docker-compose down --remove-orphans

# Remove all images
docker-compose down --rmi all

# Remove all volumes (careful!)
docker-compose down -v
```

## Security Considerations

### Environment Security
- Use `.env` file (not `.env.example`) for production secrets
- Set appropriate file permissions: `chmod 600 .env`
- Use secrets management in production (Docker Secrets, AWS Secrets Manager, etc.)

### Network Security
- Containers run in isolated network
- Only expose necessary ports
- Consider using reverse proxy for external access

### Resource Limits
```yaml# Add to docker-compose.yml for production
deploy:
  resources:
    limits:
      memory: 512M
      cpus: '0.5'
    reservations:
      memory: 256M
      cpus: '0.25'
```

## Scaling and Load Balancing

### Horizontal Scaling
```bash
# Scale specific pipelines (multiple instances)
docker-compose up -d --scale funding_rate=3
```

### Multi-Host Deployment
- Use Docker Swarm or Kubernetes for multi-host deployment
- Consider database connection pooling
- Implement load balancing for high-traffic pipelines

## Backup and Recovery

### Data Backup
```bash
# Backup database (example with MySQL)
docker-compose exec database mysqldump -u root -p coinglass_automation > backup.sql

# Backup configuration
cp .env .env.backup
```

### Container Recovery
```bash
# Rebuild and restart
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

## Best Practices

1. **Monitoring**: Set up comprehensive logging and monitoring
2. **Alerts**: Configure alerts for container failures
3. **Updates**: Regularly update base images and dependencies
4. **Testing**: Test configuration changes in staging first
5. **Documentation**: Keep deployment documentation up to date

## Support

For issues with:
- **Docker Configuration**: Check `docker-compose.yml` and `Dockerfile`
- **Application Logic**: Check application logs and error messages
- **API Issues**: Verify Coinglass API key and rate limits
- **Database Issues**: Check database connectivity and permissions