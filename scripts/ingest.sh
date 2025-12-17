#!/bin/bash

# Fluent Bit Setup and Management Script
# Usage: 
#   Setup:   ./fluent-bit.sh <host> <base64_encoded_credentials>
#            Where credentials = base64(username:password)
#   Stop:    ./fluent-bit.sh stop
#   Restart: ./fluent-bit.sh restart
#   Status:  ./fluent-bit.sh status
#   Logs:    ./fluent-bit.sh logs
#
# To encode credentials:
#   echo -n 'username:password' | base64

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# File locations
PID_FILE="./fluent-bit.pid"
LOG_FILE="./fluent-bit.log"
CONFIG_FILE="./fluent-bit.conf"

# Function to print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Function to check if Fluent Bit is running
is_running() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            return 0
        fi
    fi
    return 1
}

# Function to stop Fluent Bit
stop_fluent_bit() {
    if is_running; then
        PID=$(cat "$PID_FILE")
        print_info "Stopping Fluent Bit (PID: $PID)..."
        kill "$PID"
        
        # Wait for process to stop (max 10 seconds)
        for i in {1..10}; do
            if ! ps -p "$PID" > /dev/null 2>&1; then
                print_info "✓ Fluent Bit stopped successfully"
                rm -f "$PID_FILE"
                return 0
            fi
            sleep 1
        done
        
        # Force kill if still running
        if ps -p "$PID" > /dev/null 2>&1; then
            print_warning "Force killing Fluent Bit..."
            kill -9 "$PID"
            rm -f "$PID_FILE"
        fi
    else
        print_warning "Fluent Bit is not running"
    fi
}

# Function to show status
show_status() {
    if is_running; then
        PID=$(cat "$PID_FILE")
        print_info "✓ Fluent Bit is running (PID: $PID)"
        print_info ""
        print_info "Process details:"
        ps -p "$PID" -o pid,ppid,user,%cpu,%mem,etime,command
        print_info ""
        print_info "Log file: $LOG_FILE"
        print_info "Config file: $CONFIG_FILE"
    else
        print_warning "✗ Fluent Bit is not running"
        if [ -f "$PID_FILE" ]; then
            print_info "Cleaning up stale PID file..."
            rm -f "$PID_FILE"
        fi
    fi
}

# Function to show logs
show_logs() {
    if [ -f "$LOG_FILE" ]; then
        print_info "Showing last 50 lines of logs (Ctrl+C to exit)..."
        echo ""
        tail -50 "$LOG_FILE"
        echo ""
        print_info "To follow logs in real-time, run:"
        print_info "  tail -f $LOG_FILE"
    else
        print_error "Log file not found: $LOG_FILE"
    fi
}

# Function to get Fluent Bit binary path
get_fluent_bit_bin() {
    if [ -f /opt/fluent-bit/bin/fluent-bit ]; then
        echo "/opt/fluent-bit/bin/fluent-bit"
    elif [ -f /opt/homebrew/bin/fluent-bit ]; then
        echo "/opt/homebrew/bin/fluent-bit"
    elif [ -f /usr/local/bin/fluent-bit ]; then
        echo "/usr/local/bin/fluent-bit"
    else
        which fluent-bit 2>/dev/null || echo "fluent-bit"
    fi
}

# Function to start Fluent Bit
start_fluent_bit() {
    if is_running; then
        PID=$(cat "$PID_FILE")
        print_warning "Fluent Bit is already running (PID: $PID)"
        print_info "Use '$0 stop' to stop it first, or '$0 restart' to restart"
        exit 0
    fi
    
    if [ ! -f "$CONFIG_FILE" ]; then
        print_error "Configuration file not found: $CONFIG_FILE"
        print_error "Please run setup first with: $0 <host> <base64_credentials>"
        print_error ""
        print_error "To encode credentials:"
        print_error "  echo -n 'username:password' | base64"
        exit 1
    fi
    
    FLUENT_BIT_BIN=$(get_fluent_bit_bin)
    FINAL_VERSION=$("$FLUENT_BIT_BIN" --version 2>/dev/null | head -n1 | sed -n 's/.*v\([0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\).*/\1/p' || echo "unknown")
    
    nohup "$FLUENT_BIT_BIN" -c "$CONFIG_FILE" > "$LOG_FILE" 2>&1 &
    FLUENT_PID=$!
    echo "$FLUENT_PID" > "$PID_FILE"
    
    sleep 2
    if ps -p "$FLUENT_PID" > /dev/null 2>&1; then
        print_info "✓ Fluent Bit started successfully (PID: $FLUENT_PID)"
        print_info "To view logs: $0 logs"
        print_info "To check status: $0 status"
        print_info "To stop: $0 stop"
    else
        print_error "✗ Fluent Bit failed to start. Check logs: cat $LOG_FILE"
        rm -f "$PID_FILE"
        exit 1
    fi
}

# Function to restart Fluent Bit
restart_fluent_bit() {
    stop_fluent_bit
    sleep 2
    start_fluent_bit
}

# Function to compare versions
version_gt() {
    test "$(printf '%s\n' "$@" | sort -V | head -n 1)" != "$1"
}

# Function to get Fluent Bit version
get_fluent_bit_version() {
    local version_output
    local fluent_bit_cmd
    
    # Try different installation locations
    if [ -f /opt/fluent-bit/bin/fluent-bit ]; then
        fluent_bit_cmd="/opt/fluent-bit/bin/fluent-bit"
    elif [ -f /opt/homebrew/bin/fluent-bit ]; then
        fluent_bit_cmd="/opt/homebrew/bin/fluent-bit"
    elif [ -f /usr/local/bin/fluent-bit ]; then
        fluent_bit_cmd="/usr/local/bin/fluent-bit"
    else
        fluent_bit_cmd="fluent-bit"
    fi
    
    version_output=$($fluent_bit_cmd --version 2>/dev/null | head -n1)
    
    # Extract version using sed (portable across macOS and Linux)
    echo "$version_output" | sed -n 's/.*v\([0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\).*/\1/p' || echo "0.0.0"
}

# Detect OS
detect_os() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "macos"
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        if [ -f /etc/os-release ]; then
            . /etc/os-release
            echo "$ID"
        else
            echo "linux"
        fi
    else
        echo "unknown"
    fi
}

# Install Fluent Bit based on OS
install_fluent_bit() {
    case "$OS" in
        macos)
            print_info "Installing Fluent Bit on macOS using Homebrew..."
            if ! command -v brew &> /dev/null; then
                print_error "Homebrew is not installed. Please install Homebrew first:"
                print_error '/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"'
                exit 1
            fi
            brew install fluent-bit
            ;;
        ubuntu|debian)
            print_info "Installing Fluent Bit on Ubuntu/Debian..."
            curl https://raw.githubusercontent.com/fluent/fluent-bit/master/install.sh | sh
            ;;
        centos|rhel|fedora)
            print_info "Installing Fluent Bit on CentOS/RHEL/Fedora..."
            curl https://raw.githubusercontent.com/fluent/fluent-bit/master/install.sh | sh
            ;;
        *)
            print_error "Unsupported OS: $OS"
            print_info "Please install Fluent Bit manually from: https://docs.fluentbit.io/manual/installation/getting-started-with-fluent-bit"
            exit 1
            ;;
    esac
}

# Setup function
setup_fluent_bit() {
    local INGESTOR_HOST="$1"
    local CREDENTIALS_B64="$2"
    
    # Decode base64 credentials
    if ! CREDENTIALS=$(echo "$CREDENTIALS_B64" | base64 -d 2>/dev/null); then
        print_error "Failed to decode base64 credentials"
        print_error "Please provide credentials in base64 format: username:password"
        print_error ""
        print_error "To encode your credentials, run:"
        print_error "  echo -n 'username:password' | base64"
        exit 1
    fi
    
    # Split credentials by colon
    IFS=':' read -r INGESTOR_USERNAME INGESTOR_PASSWORD <<< "$CREDENTIALS"
    
    # Validate all fields are present
    if [ -z "$INGESTOR_HOST" ] || [ -z "$INGESTOR_USERNAME" ] || [ -z "$INGESTOR_PASSWORD" ]; then
        print_error "Invalid credentials format"
        print_error "Expected format: username:password"
        print_error ""
        print_error "To encode your credentials, run:"
        print_error "  echo -n 'username:password' | base64"
        exit 1
    fi
    
    OS=$(detect_os)
    
    # Minimum version required for node_exporter_metrics plugin
    MIN_VERSION="1.9.0"
    
    # Check if Fluent Bit is already installed
    if command -v fluent-bit &> /dev/null || [ -f /opt/homebrew/bin/fluent-bit ] || [ -f /usr/local/bin/fluent-bit ]; then
        CURRENT_VERSION=$(get_fluent_bit_version)
        
        if version_gt "$MIN_VERSION" "$CURRENT_VERSION"; then
            install_fluent_bit
            # Clear command hash to get updated binary
            hash -r 2>/dev/null || true
            NEW_VERSION=$(get_fluent_bit_version)
        fi
    else
        install_fluent_bit
        # Clear command hash to get updated binary
        hash -r 2>/dev/null || true
        NEW_VERSION=$(get_fluent_bit_version)
    fi
    
    cat > "$CONFIG_FILE" << EOF
[SERVICE]
    flush                     1
    log_level                 info

[INPUT]
    Name                      node_exporter_metrics
    Tag                       node_metrics
    Scrape_interval           2

[OUTPUT]
    Name                      opentelemetry
    Match                     node_metrics
    Host                      $INGESTOR_HOST
    Port                      443
    Metrics_uri               /v1/metrics
    Log_response_payload      True
    TLS                       On
    Http_User                 $INGESTOR_USERNAME
    Http_Passwd               $INGESTOR_PASSWORD
    Header                    X-P-Stream node-metrics
    Header                    X-P-Log-Source otel-metrics
EOF
    cat "$CONFIG_FILE" | sed "s/Http_Passwd.*/Http_Passwd               [REDACTED]/"
    
    # Start Fluent Bit
    echo ""
    start_fluent_bit
}

# Main script logic
case "${1:-}" in
    stop)
        stop_fluent_bit
        ;;
    restart)
        restart_fluent_bit
        ;;
    start)
        start_fluent_bit
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    -h|--help|help)
        echo "Fluent Bit Setup and Management Script"
        echo ""
        echo "Usage:"
        echo "  Setup and start:"
        echo "    $0 <host> <base64_encoded_credentials>"
        echo ""
        echo "  Credentials format: base64(username:password)"
        echo ""
        echo "  Management commands:"
        echo "    $0 start    - Start Fluent Bit (if config exists)"
        echo "    $0 stop     - Stop Fluent Bit"
        echo "    $0 restart  - Restart Fluent Bit"
        echo "    $0 status   - Show Fluent Bit status"
        echo "    $0 logs     - Show Fluent Bit logs"
        echo ""
        echo "To encode your credentials:"
        echo "  echo -n 'username:password' | base64"
        echo ""
        echo "Example:"
        echo "  CREDS=\$(echo -n 'user@email.com:password123' | base64)"
        echo "  $0 example.parseable.com \$CREDS"
        ;;
    *)
        # If not a command, treat as setup parameters
        if [ $# -ne 2 ]; then
            print_error "Usage: $0 <host> <base64_encoded_credentials>"
            print_error "   Or: $0 [start|stop|restart|status|logs|help]"
            print_error ""
            print_error "Credentials format: base64(username:password)"
            print_error ""
            print_error "To encode your credentials:"
            print_error "  echo -n 'username:password' | base64"
            print_error ""
            print_error "Example:"
            print_error "  CREDS=\$(echo -n 'hello@parseable.com:NH7oCUju' | base64)"
            print_error "  $0 ec9cfee0-2fd4-45eb-8209-d7cd992c4bcc-ingestor.workspace-staging.parseable.com \$CREDS"
            print_error ""
            print_error "Management commands:"
            print_error "  $0 status   - Check if running"
            print_error "  $0 stop     - Stop Fluent Bit"
            print_error "  $0 restart  - Restart Fluent Bit"
            print_error "  $0 logs     - View logs"
            exit 1
        fi
        setup_fluent_bit "$1" "$2"
        ;;
esac