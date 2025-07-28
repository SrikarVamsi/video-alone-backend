#!/bin/bash

# Real-Time Analytics Pipeline Startup Script
# This script handles environment setup, dependency checks, and application startup

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
APP_NAME="Real-Time Analytics Pipeline"
PYTHON_VERSION="3.8"
REQUIREMENTS_FILE="requirements.txt"
ENV_FILE=".env"
LOG_DIR="logs"
PID_FILE="app.pid"

# Functions
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

check_python() {
    log_info "Checking Python version..."
    
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 is not installed. Please install Python 3.8 or higher."
        exit 1
    fi
    
    PYTHON_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    log_info "Found Python version: $PYTHON_VER"
    
    if ! python3 -c "import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)"; then
        log_error "Python 3.8 or higher is required. Found: $PYTHON_VER"
        exit 1
    fi
    
    log_success "Python version check passed"
}

check_kafka() {
    log_info "Checking Kafka connectivity..."
    
    # Check if Kafka is running (assuming default port 9092)
    KAFKA_HOST=${KAFKA_BOOTSTRAP_SERVERS:-"localhost:9092"}
    
    if ! timeout 5 bash -c "cat < /dev/null > /dev/tcp/${KAFKA_HOST/:/ }"; then
        log_warning "Cannot connect to Kafka at $KAFKA_HOST"
        log_warning "Please ensure Kafka is running before starting the application"
        log_warning "You can start Kafka using: docker-compose up -d kafka"
    else
        log_success "Kafka connectivity check passed"
    fi
}

setup_environment() {
    log_info "Setting up environment..."
    
    # Create logs directory
    mkdir -p $LOG_DIR
    
    # Check if .env file exists
    if [ ! -f "$ENV_FILE" ]; then
        log_warning ".env file not found. Creating from template..."
        cat > $ENV_FILE << EOF
# Environment Configuration
ENVIRONMENT=development
LOG_LEVEL=INFO
LOG_FORMAT=structured

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
API_WORKERS=1

# WebSocket Configuration
WS_HOST=0.0.0.0
WS_PORT=8001

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CONSUMER_GROUP=analytics-pipeline
KAFKA_AUTO_OFFSET_RESET=latest

# Alert Configuration
ALERT_RETENTION_HOURS=24
ALERT_MAX_MEMORY_SIZE=10000

# Security Configuration
JWT_SECRET_KEY=your-secret-key-change-this-in-production
JWT_ALGORITHM=HS256
JWT_EXPIRATION_MINUTES=30

# CORS Configuration
ALLOWED_ORIGINS=["http://localhost:3000", "http://localhost:8080"]
ALLOWED_HOSTS=["localhost", "127.0.0.1"]

# Redis Configuration (optional)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
EOF
        log_success ".env file created. Please review and update the configuration."
    else
        log_success ".env file found"
    fi
}

install_dependencies() {
    log_info "Installing Python dependencies..."
    
    if [ ! -f "$REQUIREMENTS_FILE" ]; then
        log_error "requirements.txt not found. Please ensure it exists in the current directory."
        exit 1
    fi
    
    # Check if virtual environment exists
    if [ ! -d "venv" ]; then
        log_info "Creating virtual environment..."
        python3 -m venv venv
        log_success "Virtual environment created"
    fi
    
    # Activate virtual environment
    source venv/bin/activate
    
    # Upgrade pip
    pip install --upgrade pip
    
    # Install dependencies
    pip install -r $REQUIREMENTS_FILE
    
    log_success "Dependencies installed"
}

check_dependencies() {
    log_info "Checking required dependencies..."
    
    source venv/bin/activate
    
    # Check critical dependencies
    CRITICAL_DEPS=("fastapi" "uvicorn" "kafka-python" "websockets" "pydantic" "redis")
    
    for dep in "${CRITICAL_DEPS[@]}"; do
        if ! pip show "$dep" &> /dev/null; then
            log_error "Critical dependency '$dep' not found. Please run: pip install -r requirements.txt"
            exit 1
        fi
    done
    
    log_success "All critical dependencies found"
}

start_application() {
    log_info "Starting $APP_NAME..."
    
    # Activate virtual environment
    source venv/bin/activate
    
    # Export environment variables
    export PYTHONPATH="${PYTHONPATH}:$(pwd)"
    
    # Check if app is already running
    if [ -f "$PID_FILE" ]; then
        OLD_PID=$(cat $PID_FILE)
        if ps -p $OLD_PID > /dev/null 2>&1; then
            log_warning "Application is already running with PID $OLD_PID"
            log_info "To stop the application, run: kill $OLD_PID"
            exit 1
        else
            log_warning "Stale PID file found. Removing..."
            rm -f $PID_FILE
        fi
    fi
    
    # Start the application
    log_info "Launching application..."
    
    if [ "$1" == "dev" ]; then
        log_info "Starting in development mode..."
        python3 main.py
    elif [ "$1" == "background" ]; then
        log_info "Starting in background mode..."
        nohup python3 main.py > $LOG_DIR/app.log 2>&1 &
        echo $! > $PID_FILE
        log_success "Application started in background with PID $(cat $PID_FILE)"
        log_info "Logs are available in $LOG_DIR/app.log"
        log_info "To stop the application, run: $0 stop"
    else
        log_info "Starting in foreground mode..."
        python3 main.py
    fi
}

stop_application() {
    log_info "Stopping $APP_NAME..."
    
    if [ -f "$PID_FILE" ]; then
        PID=$(cat $PID_FILE)
        if ps -p $PID > /dev/null 2>&1; then
            log_info "Sending SIGTERM to PID $PID..."
            kill -TERM $PID
            
            # Wait for graceful shutdown
            sleep 5
            
            # Check if process is still running
            if ps -p $PID > /dev/null 2>&1; then
                log_warning "Process still running. Sending SIGKILL..."
                kill -KILL $PID
            fi
            
            rm -f $PID_FILE
            log_success "Application stopped"
        else
            log_warning "Process with PID $PID not found"
            rm -f $PID_FILE
        fi
    else
        log_warning "PID file not found. Application may not be running."
    fi
}

show_status() {
    log_info "Checking application status..."
    
    if [ -f "$PID_FILE" ]; then
        PID=$(cat $PID_FILE)
        if ps -p $PID > /dev/null 2>&1; then
            log_success "Application is running with PID $PID"
            log_info "Memory usage: $(ps -o rss= -p $PID | awk '{print $1/1024 " MB"}')"
            log_info "CPU usage: $(ps -o %cpu= -p $PID)%"
        else
            log_warning "PID file exists but process is not running"
            rm -f $PID_FILE
        fi
    else
        log_info "Application is not running"
    fi
}

show_logs() {
    if [ -f "$LOG_DIR/app.log" ]; then
        tail -f $LOG_DIR/app.log
    else
        log_warning "Log file not found. Application may not be running in background mode."
    fi
}

show_help() {
    echo "Usage: $0 {start|start-dev|start-bg|stop|restart|status|logs|install|help}"
    echo ""
    echo "Commands:"
    echo "  start      - Start application in foreground mode"
    echo "  start-dev  - Start application in development mode with reload"
    echo "  start-bg   - Start application in background mode"
    echo "  stop       - Stop the application"
    echo "  restart    - Restart the application"
    echo "  status     - Show application status"
    echo "  logs       - Show application logs (background mode only)"
    echo "  install    - Install dependencies and setup environment"
    echo "  help       - Show this help message"
}

# Main script logic
case "$1" in
    start)
        check_python
        check_dependencies
        setup_environment
        check_kafka
        start_application
        ;;
    start-dev)
        check_python
        check_dependencies
        setup_environment
        check_kafka
        start_application "dev"
        ;;
    start-bg)
        check_python
        check_dependencies
        setup_environment
        check_kafka
        start_application "background"
        ;;
    stop)
        stop_application
        ;;
    restart)
        stop_application
        sleep 2
        start_application "background"
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    install)
        check_python
        setup_environment
        install_dependencies
        check_dependencies
        log_success "Installation completed successfully!"
        log_info "You can now start the application with: $0 start"
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        log_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac