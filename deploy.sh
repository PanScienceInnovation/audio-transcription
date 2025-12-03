#!/bin/bash

# Audio Transcription App Deployment Script
# This script builds and deploys the application using Docker Compose

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
CONTAINER_NAME="audio-transcription"
HEALTH_CHECK_URL="http://localhost:5002/api/health"
HEALTH_CHECK_MAX_RETRIES=30
HEALTH_CHECK_INTERVAL=5

# Function to print colored messages
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check prerequisites
check_prerequisites() {
    print_info "Checking prerequisites..."
    
    local missing_deps=()
    local docker_ok=false
    local compose_ok=false
    
    # Check for Docker
    if command_exists docker; then
        docker_ok=true
    else
        missing_deps+=("docker")
    fi
    
    # Check for Docker Compose (only if Docker exists)
    if [ "$docker_ok" = true ]; then
        if command_exists docker-compose; then
            compose_ok=true
        elif docker compose version >/dev/null 2>&1; then
            compose_ok=true
        else
            missing_deps+=("docker-compose or 'docker compose'")
        fi
    else
        # If docker doesn't exist, we can't check compose, so add it to missing
        missing_deps+=("docker-compose or 'docker compose'")
    fi
    
    if [ ${#missing_deps[@]} -ne 0 ]; then
        print_error "Missing required dependencies: ${missing_deps[*]}"
        echo ""
        print_info "Installation instructions:"
        echo "  For Ubuntu/Debian:"
        echo "    1. sudo apt update"
        echo "    2. sudo apt install -y docker.io docker-compose"
        echo "    3. sudo systemctl start docker"
        echo "    4. sudo systemctl enable docker"
        echo "    5. sudo usermod -aG docker \$USER"
        echo "    6. Log out and back in, or run: newgrp docker"
        echo ""
        echo "  Note: After installation, verify with:"
        echo "    docker --version"
        echo "    docker-compose --version"
        echo ""
        exit 1
    fi
    
    # Check if Docker daemon is running
    if ! docker info >/dev/null 2>&1; then
        print_error "Docker daemon is not running. Please start Docker and try again."
        print_info "Try: sudo systemctl start docker"
        exit 1
    fi
    
    print_success "All prerequisites met"
}

# Function to check required files
check_required_files() {
    print_info "Checking required files..."
    
    local missing_files=()
    
    # Check backend .env file
    if [ ! -f "backend/.env" ]; then
        missing_files+=("backend/.env")
    fi
    
    # Check frontend .env file (optional but recommended)
    if [ ! -f "frontend/.env" ]; then
        print_warning "frontend/.env not found (optional but recommended for build-time variables)"
    fi
    
    # Check GCP credentials
    if [ ! -f "gcp-credentials_bkp.json" ]; then
        print_warning "gcp-credentials_bkp.json not found (required for Google Cloud services)"
    fi
    
    if [ ${#missing_files[@]} -ne 0 ]; then
        print_error "Missing required files: ${missing_files[*]}"
        print_error "Please create these files before deploying."
        exit 1
    fi
    
    print_success "Required files present"
}

# Function to stop existing containers
stop_existing_containers() {
    print_info "Bringing down containers..."
    
    # Use docker compose if available, otherwise docker-compose
    if docker compose version >/dev/null 2>&1; then
        docker compose down 2>/dev/null || true
    else
        docker-compose down 2>/dev/null || true
    fi
    
    # Also try to stop by container name (in case docker-compose wasn't used)
    if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        print_info "Stopping container: ${CONTAINER_NAME}"
        docker stop "${CONTAINER_NAME}" 2>/dev/null || true
        docker rm "${CONTAINER_NAME}" 2>/dev/null || true
    fi
    
    print_success "Containers brought down"
}

# Function to build Docker image
build_image() {
    print_info "Building Docker image..."
    
    if docker compose version >/dev/null 2>&1; then
        docker compose build
    else
        docker-compose build
    fi
    
    if [ $? -eq 0 ]; then
        print_success "Docker image built successfully"
    else
        print_error "Failed to build Docker image"
        exit 1
    fi
}

# Function to start containers
start_containers() {
    print_info "Starting containers..."
    
    if docker compose version >/dev/null 2>&1; then
        docker compose up -d
    else
        docker-compose up -d
    fi
    
    if [ $? -eq 0 ]; then
        print_success "Containers started successfully"
    else
        print_error "Failed to start containers"
        exit 1
    fi
}

# Function to wait for health check
wait_for_health() {
    print_info "Waiting for application to be healthy..."
    
    local retries=0
    local health_ok=false
    
    while [ $retries -lt $HEALTH_CHECK_MAX_RETRIES ]; do
        if curl -f -s "${HEALTH_CHECK_URL}" >/dev/null 2>&1; then
            health_ok=true
            break
        fi
        
        retries=$((retries + 1))
        if [ $retries -lt $HEALTH_CHECK_MAX_RETRIES ]; then
            echo -n "."
            sleep $HEALTH_CHECK_INTERVAL
        fi
    done
    
    echo ""  # New line after dots
    
    if [ "$health_ok" = true ]; then
        print_success "Application is healthy and ready"
        return 0
    else
        print_warning "Health check did not pass after ${HEALTH_CHECK_MAX_RETRIES} retries"
        print_warning "The application may still be starting. Check logs with: docker compose logs"
        return 1
    fi
}

# Function to show container status
show_status() {
    print_info "Container status:"
    
    if docker compose version >/dev/null 2>&1; then
        docker compose ps
    else
        docker-compose ps
    fi
    
    echo ""
    print_info "Recent logs (last 20 lines):"
    if docker compose version >/dev/null 2>&1; then
        docker compose logs --tail=20
    else
        docker-compose logs --tail=20
    fi
}

# Main deployment function
main() {
    echo ""
    print_info "========================================="
    print_info "Audio Transcription App Deployment"
    print_info "========================================="
    echo ""
    
    # Parse command line arguments
    SKIP_BUILD=false
    SKIP_HEALTH_CHECK=false
    FOLLOW_LOGS=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --skip-build)
                SKIP_BUILD=true
                shift
                ;;
            --skip-health-check)
                SKIP_HEALTH_CHECK=true
                shift
                ;;
            --follow-logs)
                FOLLOW_LOGS=true
                shift
                ;;
            --help|-h)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --skip-build          Skip building the Docker image"
                echo "  --skip-health-check   Skip waiting for health check"
                echo "  --follow-logs         Follow logs after deployment"
                echo "  --help, -h            Show this help message"
                echo ""
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
    
    # Run deployment steps in order: down -> build -> up
    check_prerequisites
    check_required_files
    
    # Step 1: Bring down existing containers
    stop_existing_containers
    
    # Step 2: Build Docker image
    if [ "$SKIP_BUILD" = false ]; then
        build_image
    else
        print_warning "Skipping build step"
    fi
    
    # Step 3: Start containers
    start_containers
    
    if [ "$SKIP_HEALTH_CHECK" = false ]; then
        wait_for_health
    else
        print_warning "Skipping health check"
    fi
    
    show_status
    
    echo ""
    print_success "========================================="
    print_success "Deployment completed!"
    print_success "========================================="
    echo ""
    print_info "Application URL: http://localhost:5002"
    print_info "Health check: ${HEALTH_CHECK_URL}"
    echo ""
    print_info "Useful commands:"
    echo "  View logs:        docker compose logs -f"
    echo "  Stop service:     docker compose down"
    echo "  Restart service:  docker compose restart"
    echo "  Check status:     docker compose ps"
    echo ""
    
    if [ "$FOLLOW_LOGS" = true ]; then
        print_info "Following logs (Ctrl+C to exit)..."
        if docker compose version >/dev/null 2>&1; then
            docker compose logs -f
        else
            docker-compose logs -f
        fi
    fi
}

# Run main function
main "$@"

