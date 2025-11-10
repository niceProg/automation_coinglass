# Fix: /app/entrypoint.sh Not Found Error

## Problem
The Docker containers are showing the error:
```
/bin/bash: /app/entrypoint.sh: No such file or directory
```

This happens because the Docker images were built before `entrypoint.sh` was created.

## Solution

You need to rebuild the Docker images. Choose one of the methods below:

### Method 1: Quick Rebuild Script (Recommended)

Run the rebuild script that will automatically stop services, clean up, and rebuild:

```bash
./rebuild.sh
```

Then select which profile to deploy when prompted.

### Method 2: Manual Rebuild

Stop all services and rebuild manually:

```bash
# Step 1: Stop all services
./stop.sh --all

# Step 2: Remove old images (optional but recommended)
docker-compose down --rmi all --remove-orphans

# Step 3: Rebuild and deploy
./deploy.sh --all-profiles
```

### Method 3: Deploy Script with Clean Build

The deploy.sh script now checks for entrypoint.sh and rebuilds automatically:

```bash
# For all services
./deploy.sh --all-profiles

# For specific profile
./deploy.sh --derivatives
./deploy.sh --exchange
./deploy.sh --spot
./deploy.sh --etf
./deploy.sh --trading
```

## Verification

After rebuilding, verify the services are running:

```bash
# Check running containers
docker-compose ps

# Check logs for any errors
docker-compose logs -f

# Check specific service
docker-compose logs -f long_short_ratio_binance
```

## What Was Fixed

1. **Dockerfile**: Updated to copy `entrypoint.sh` explicitly before copying other files
2. **deploy.sh**: Now verifies `entrypoint.sh` exists before building
3. **entrypoint.sh**: Created with proper execute permissions
4. **rebuild.sh**: New script for quick rebuilds when needed

## Expected Behavior After Fix

Each service should show logs like:
```
==========================================
🚀 Starting Coinglass Pipeline Runner
==========================================
Pipeline: long_short_ratio
Exchange Filter: Binance
Delay: 5 seconds
==========================================

⏰ 2025-11-04 10:30:00 - Running long_short_ratio for Binance
✅ Pipeline completed successfully
💤 Sleeping for 5 seconds...
```

## Troubleshooting

If you still see errors after rebuilding:

1. **Check if entrypoint.sh exists:**
   ```bash
   ls -la entrypoint.sh
   ```

2. **Check if it's executable:**
   ```bash
   chmod +x entrypoint.sh
   ```

3. **Force rebuild without cache:**
   ```bash
   docker-compose build --no-cache
   ```

4. **Check Docker build logs:**
   ```bash
   docker-compose build --no-cache --progress=plain
   ```

5. **Verify the file is in the image:**
   ```bash
   docker-compose run --rm long_short_ratio_binance ls -la /app/entrypoint.sh
   ```
