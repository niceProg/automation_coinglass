# Docker Deployment Guide

This guide will help you deploy the Automation Coinglass application using Docker on your server.

## 📋 Prerequisites

Before you begin, ensure you have the following installed on your server:

- **Docker** (version 20.10 or later)
- **Docker Compose** (version 1.29 or later)
- **Git** (to clone the repository)

### Installing Docker

#### Ubuntu/Debian
```bash
# Update package index
sudo apt-get update

# Install required packages
sudo apt-get install \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

# Add Docker's official GPG key
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Set up repository
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker Engine
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-compose-plugin
```

#### CentOS/RHEL/Fedora
```bash
# Install Docker
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
sudo yum install docker-ce docker-ce-cli containerd.io docker-compose-plugin
```

#### Start Docker Service
```bash
sudo systemctl start docker
sudo systemctl enable docker
```

## 🚀 Quick Deployment

### 1. Clone the Repository

```bash
git clone <your-repository-url>
cd automation_coinglass
```

### 2. Run the Deploy Script

The easiest way to deploy is using the provided `deploy.sh` script:

```bash
# Make the script executable
chmod +x deploy.sh

# Run the deployment
./deploy.sh
```

This script will automatically:
- Check Docker and Docker Compose installation
- Build the Docker image
- Create necessary directories
- Start all services
- Show service status and logs

## 📁 Project Structure

```
automation_coinglass/
├── docker-compose.yml      # Docker Compose configuration
├── Dockerfile             # Docker image build instructions
├── deploy.sh              # Deployment script
├── .env.example           # Environment variables template
├── src/                   # Application source code
├── logs/                  # Application logs (auto-created)
└── data/                  # Persistent data (auto-created)
```

## ⚙️ Configuration

### Environment Variables

1. Copy the environment template:
```bash
cp .env.example .env
```

2. Edit the `.env` file with your configuration:
```bash
nano .env
```

Example `.env` file:
```env
# Application Configuration
NODE_ENV=production
PORT=3000

# Database Configuration
DB_HOST=postgres
DB_PORT=5432
DB_NAME=coinglass_db
DB_USER=your_db_user
DB_PASSWORD=your_secure_password

# API Keys (if needed)
API_KEY=your_api_key_here
```

### Docker Compose Configuration

The `docker-compose.yml` file defines your services. Here's an example structure:

```yaml
version: '3.8'

services:
  app:
    build: .
    container_name: coinglass_app
    restart: unless-stopped
    ports:
      - "3000:3000"
    environment:
      - NODE_ENV=${NODE_ENV}
      - DB_HOST=${DB_HOST}
    volumes:
      - ./logs:/app/logs
      - ./data:/app/data
    depends_on:
      - postgres
    networks:
      - coinglass_network

  postgres:
    image: postgres:15-alpine
    container_name: coinglass_db
    restart: unless-stopped
    environment:
      POSTGRES_DB: ${DB_NAME}
      POSTGRES_USER: ${DB_USER}
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - coinglass_network

volumes:
  postgres_data:

networks:
  coinglass_network:
    driver: bridge
```

## 🔧 Manual Deployment Commands

If you prefer to run commands manually instead of using `deploy.sh`:

### 1. Build the Docker Image
```bash
docker-compose build
```

### 2. Start Services
```bash
# Start all services in detached mode
docker-compose up -d

# Start services with logs
docker-compose up
```

### 3. Check Service Status
```bash
docker-compose ps
```

### 4. View Logs
```bash
# View all logs
docker-compose logs

# Follow logs in real-time
docker-compose logs -f

# View logs for specific service
docker-compose logs -f app
```

## 🛠️ Common Docker Commands

### Container Management
```bash
# Stop all services
docker-compose down

# Stop and remove volumes (WARNING: This deletes data)
docker-compose down -v

# Restart services
docker-compose restart

# Rebuild and restart
docker-compose up -d --build

# Execute commands in container
docker-compose exec app bash
docker-compose exec postgres psql -U username -d database_name
```

### Image Management
```bash
# List all images
docker images

# Remove unused images
docker image prune

# Force remove all unused images
docker image prune -a
```

### Volume Management
```bash
# List volumes
docker volume ls

# Inspect volume
docker volume inspect volume_name

# Remove unused volumes
docker volume prune
```

## 🔍 Troubleshooting

### Common Issues

1. **Port Already in Use**
   ```bash
   # Check what's using the port
   sudo netstat -tlnp | grep :3000

   # Stop the service using the port or change the port in docker-compose.yml
   ```

2. **Permission Denied**
   ```bash
   # Add user to docker group
   sudo usermod -aG docker $USER

   # Logout and login again for changes to take effect
   ```

3. **Out of Disk Space**
   ```bash
   # Clean up Docker
   docker system prune -a
   ```

4. **Container Won't Start**
   ```bash
   # Check container logs
   docker-compose logs service_name

   # Inspect container
   docker inspect container_name
   ```

### Health Checks

Check if your application is running:
```bash
# Check if container is running
docker-compose ps

# Test application endpoint
curl http://localhost:3000/health

# Check resource usage
docker stats
```

## 🔄 Updating the Application

To update your application to a new version:

1. **Pull Latest Changes**
   ```bash
   git pull origin main
   ```

2. **Redeploy**
   ```bash
   # Using the script
   ./deploy.sh

   # Or manually
   docker-compose down
   docker-compose build --no-cache
   docker-compose up -d
   ```

3. **Run Database Migrations (if needed)**
   ```bash
   docker-compose exec app npm run migrate
   ```

## 🔒 Security Best Practices

1. **Use Environment Variables**: Never commit secrets to your repository
2. **Regular Updates**: Keep Docker images updated
3. **Network Isolation**: Use Docker networks to isolate services
4. **Resource Limits**: Set memory and CPU limits in production
5. **Regular Backups**: Backup volumes and databases regularly

Example with resource limits:
```yaml
services:
  app:
    deploy:
      resources:
        limits:
          cpus: '1.0'
          memory: 1G
        reservations:
          cpus: '0.5'
          memory: 512M
```

## 📊 Monitoring

### Basic Monitoring Commands
```bash
# Real-time resource usage
docker stats

# Container events
docker events

# Disk usage
docker system df
```

### Log Management
```bash
# Configure log rotation in docker-compose.yml
logging:
  driver: "json-file"
  options:
    max-size: "10m"
    max-file: "3"
```

## 🆘 Getting Help

- **Docker Documentation**: https://docs.docker.com
- **Docker Compose Documentation**: https://docs.docker.com/compose
- **Project Issues**: Check the project's GitHub Issues page

## 📝 Next Steps

After successful deployment:

1. [ ] Configure your reverse proxy (Nginx/Apache)
2. [ ] Set up SSL certificates (Let's Encrypt)
3. [ ] Configure monitoring and alerts
4. [ ] Set up automated backups
5. [ ] Configure CI/CD pipeline

---

**Happy Dockering! 🐳**