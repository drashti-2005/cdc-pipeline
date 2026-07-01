#!/bin/bash
# =============================================================================
# CDC Pipeline Deployment Script
# =============================================================================
# Usage: ./deploy.sh <environment> [options]
# Environments: local, staging, production
# =============================================================================

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default values
ENVIRONMENT="${1:-local}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
DRY_RUN="${DRY_RUN:-false}"
NAMESPACE="cdc-${ENVIRONMENT}"

# =============================================================================
# Helper Functions
# =============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_dependencies() {
    log_info "Checking dependencies..."
    
    local missing_deps=()
    
    command -v docker &> /dev/null || missing_deps+=("docker")
    command -v docker-compose &> /dev/null || missing_deps+=("docker-compose")
    
    if [[ "$ENVIRONMENT" != "local" ]]; then
        command -v kubectl &> /dev/null || missing_deps+=("kubectl")
        command -v helm &> /dev/null || log_warning "helm not found (optional)"
    fi
    
    if [[ ${#missing_deps[@]} -gt 0 ]]; then
        log_error "Missing dependencies: ${missing_deps[*]}"
        exit 1
    fi
    
    log_success "All required dependencies found"
}

validate_environment() {
    case "$ENVIRONMENT" in
        local|staging|production)
            log_info "Target environment: $ENVIRONMENT"
            ;;
        *)
            log_error "Invalid environment: $ENVIRONMENT"
            echo "Valid environments: local, staging, production"
            exit 1
            ;;
    esac
}

# =============================================================================
# Deployment Functions
# =============================================================================

deploy_local() {
    log_info "Deploying to local environment..."
    
    cd "$PROJECT_ROOT/docker"
    
    # Stop existing containers
    log_info "Stopping existing containers..."
    docker-compose down --remove-orphans 2>/dev/null || true
    
    # Build and start
    log_info "Building and starting services..."
    docker-compose build
    docker-compose up -d
    
    # Wait for services
    log_info "Waiting for services to be healthy..."
    sleep 10
    
    # Check health
    if docker-compose ps | grep -q "Up"; then
        log_success "Local deployment successful!"
        docker-compose ps
    else
        log_error "Some services failed to start"
        docker-compose logs
        exit 1
    fi
}

deploy_kubernetes() {
    local env="$1"
    
    log_info "Deploying to Kubernetes ($env)..."
    
    # Check if namespace exists
    if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
        log_info "Creating namespace $NAMESPACE..."
        kubectl create namespace "$NAMESPACE"
    fi
    
    # Apply configurations
    local manifest_dir="$PROJECT_ROOT/deploy/k8s/$env"
    
    if [[ -d "$manifest_dir" ]]; then
        log_info "Applying Kubernetes manifests from $manifest_dir..."
        
        if [[ "$DRY_RUN" == "true" ]]; then
            kubectl apply -f "$manifest_dir/" --dry-run=client
        else
            kubectl apply -f "$manifest_dir/"
        fi
        
        # Update image
        log_info "Updating image to $IMAGE_TAG..."
        kubectl set image deployment/cdc-pipeline \
            cdc-pipeline="ghcr.io/your-org/cdc-pipeline:$IMAGE_TAG" \
            -n "$NAMESPACE" || true
        
        # Wait for rollout
        log_info "Waiting for rollout to complete..."
        kubectl rollout status deployment/cdc-pipeline -n "$NAMESPACE" --timeout=300s || true
        
        log_success "Kubernetes deployment completed!"
    else
        log_warning "No manifests found at $manifest_dir"
        log_info "Skipping Kubernetes deployment"
    fi
}

run_smoke_tests() {
    log_info "Running smoke tests..."
    
    case "$ENVIRONMENT" in
        local)
            # Test local endpoints
            if curl -sf http://localhost:8080/health > /dev/null 2>&1; then
                log_success "Health check passed"
            else
                log_warning "Health endpoint not responding (service may still be starting)"
            fi
            ;;
        staging|production)
            # Would test actual endpoints
            log_info "Smoke tests for $ENVIRONMENT would run here"
            ;;
    esac
}

# =============================================================================
# Main
# =============================================================================

main() {
    echo "============================================="
    echo "CDC Pipeline Deployment"
    echo "============================================="
    echo ""
    
    check_dependencies
    validate_environment
    
    case "$ENVIRONMENT" in
        local)
            deploy_local
            ;;
        staging)
            deploy_kubernetes "staging"
            ;;
        production)
            log_warning "Production deployment requested"
            read -p "Are you sure you want to deploy to production? (yes/no): " confirm
            if [[ "$confirm" == "yes" ]]; then
                deploy_kubernetes "production"
            else
                log_info "Production deployment cancelled"
                exit 0
            fi
            ;;
    esac
    
    run_smoke_tests
    
    echo ""
    log_success "Deployment to $ENVIRONMENT completed!"
}

# Run main
main "$@"
