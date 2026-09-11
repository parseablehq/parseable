#!/bin/bash

# OpenTelemetry Collector host-metrics setup and management script
# Usage:
#   Setup:   ./ingest.sh <host[:port]> <stream> <api_key> [tenant_id]
#   Stop:    ./ingest.sh stop
#   Restart: ./ingest.sh restart
#   Status:  ./ingest.sh status
#   Logs:    ./ingest.sh logs

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
ACCENT='\033[38;2;158;158;240m'
OK='\033[38;2;52;211;153m'
BOLD='\033[1m'
NC='\033[0m'

COLLECTOR_VERSION="0.157.0"
COLLECTOR_DIR="./otelcol"
COLLECTOR_BIN="$COLLECTOR_DIR/otelcol"
PID_FILE="./otelcol.pid"
LOG_FILE="./otelcol.log"
CONFIG_FILE="./otelcol.yaml"

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_parseable_banner() {
    echo ""
    printf '%b%s%b\n' "$ACCENT" ' ____   _    ____  ____  _____    _    ____  _     _____' "$NC"
    printf '%b%s%b\n' "$ACCENT" '|  _ \ / \  |  _ \/ ___|| ____|  / \  | __ )| |   | ____|' "$NC"
    printf '%b%s%b\n' "$ACCENT" '| |_) / _ \ | |_) \___ \|  _|   / _ \ |  _ \| |   |  _|' "$NC"
    printf '%b%s%b\n' "$ACCENT" '|  __/ ___ \|  _ < ___) | |___ / ___ \| |_) | |___| |___' "$NC"
    printf '%b%s%b\n' "$ACCENT" '|_| /_/   \_\_| \_\____/|_____/_/   \_\____/|_____|_____|' "$NC"
    echo ""
    echo -e "${BOLD}Host metrics setup${NC}"
    echo "Installing and configuring the metrics agent for Parseable..."
    echo ""
}

print_setup_complete() {
    local stream_name="$1"

    echo ""
    echo -e "${OK}${BOLD}✓ You're all set!${NC}"
    echo "Host metrics are now being sent to Parseable as OTLP JSON."
    echo -e "Dataset: ${BOLD}${stream_name}${NC}"
    echo "Return to Parseable and click Continue to verify your data."
}

is_running() {
    local process_command
    local config_base

    config_base=$(basename "$CONFIG_FILE")

    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if [[ "$PID" =~ ^[0-9]+$ ]] && ps -p "$PID" > /dev/null 2>&1; then
            process_command=$(ps -p "$PID" -o command= 2>/dev/null || true)
            case "$process_command" in
                *otelcol*"$config_base"*) return 0 ;;
            esac
        fi
    fi
    return 1
}

stop_collector() {
    if is_running; then
        PID=$(cat "$PID_FILE")
        print_info "Stopping OpenTelemetry Collector (PID: $PID)..."
        if ! kill "$PID"; then
            print_error "Failed to stop OpenTelemetry Collector; PID file was preserved"
            return 1
        fi

        for _ in {1..10}; do
            if ! ps -p "$PID" > /dev/null 2>&1; then
                print_info "✓ OpenTelemetry Collector stopped successfully"
                rm -f "$PID_FILE"
                return 0
            fi
            sleep 1
        done

        if is_running; then
            print_warning "Force killing OpenTelemetry Collector..."
            if ! kill -9 "$PID"; then
                print_error "Failed to force stop OpenTelemetry Collector; PID file was preserved"
                return 1
            fi

            for _ in {1..5}; do
                if ! ps -p "$PID" > /dev/null 2>&1; then
                    rm -f "$PID_FILE"
                    print_info "✓ OpenTelemetry Collector stopped successfully"
                    return 0
                fi
                sleep 1
            done

            print_error "OpenTelemetry Collector did not stop; PID file was preserved"
            return 1
        fi

        rm -f "$PID_FILE"
        print_info "✓ OpenTelemetry Collector stopped successfully"
    else
        print_warning "OpenTelemetry Collector is not running"
        if [ -f "$PID_FILE" ]; then
            rm -f "$PID_FILE"
        fi
    fi
}

show_status() {
    if is_running; then
        PID=$(cat "$PID_FILE")
        print_info "✓ OpenTelemetry Collector is running (PID: $PID)"
        print_info ""
        print_info "Process details:"
        ps -p "$PID" -o pid,ppid,user,%cpu,%mem,etime,command
        print_info ""
        print_info "Log file: $LOG_FILE"
        print_info "Config file: $CONFIG_FILE"
    else
        print_warning "✗ OpenTelemetry Collector is not running"
        if [ -f "$PID_FILE" ]; then
            print_info "Cleaning up stale PID file..."
            rm -f "$PID_FILE"
        fi
    fi
}

show_logs() {
    if [ -f "$LOG_FILE" ]; then
        print_info "Showing last 80 OpenTelemetry Collector log lines..."
        echo ""
        tail -80 "$LOG_FILE"
        echo ""
        print_info "To follow logs in real-time, run:"
        print_info "  tail -f $LOG_FILE"
    else
        print_error "Log file not found: $LOG_FILE"
    fi
}

install_collector() {
    local collector_os
    local collector_arch
    local expected_hash
    local archive_name
    local download_url
    local temp_dir
    local archive_path
    local actual_hash
    local extracted_bin

    if [ -x "$COLLECTOR_BIN" ] && "$COLLECTOR_BIN" --version 2>/dev/null | grep -q "$COLLECTOR_VERSION"; then
        return 0
    fi

    case "$(uname -s)" in
        Linux) collector_os="linux" ;;
        Darwin) collector_os="darwin" ;;
        *)
            print_error "Unsupported OS: $(uname -s)"
            exit 1
            ;;
    esac

    case "$(uname -m)" in
        x86_64|amd64) collector_arch="amd64" ;;
        arm64|aarch64) collector_arch="arm64" ;;
        *)
            print_error "Unsupported CPU architecture: $(uname -m)"
            exit 1
            ;;
    esac

    case "$collector_os/$collector_arch" in
        linux/amd64) expected_hash="2937cf24892af55b143c072fddece17862239cf78280620029276493eb81beae" ;;
        linux/arm64) expected_hash="59b63b99bab315509375fee76e22a9065eb9d9ba0a8995f8e985d60ca50d34ea" ;;
        darwin/amd64) expected_hash="974420dce3aa9ba22b9e4e26cd68761f91e439bea441482166f19ece3fc186c3" ;;
        darwin/arm64) expected_hash="1ea74db004f247948db7f5f99bc88a38a3c017cd5fb9b3a1fb62a98af0caa8c8" ;;
    esac

    if ! command -v curl > /dev/null 2>&1; then
        print_error "curl is required to install OpenTelemetry Collector"
        exit 1
    fi

    archive_name="otelcol_${COLLECTOR_VERSION}_${collector_os}_${collector_arch}.tar.gz"
    download_url="https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v${COLLECTOR_VERSION}/${archive_name}"
    temp_dir=$(mktemp -d)
    archive_path="$temp_dir/$archive_name"
    INGEST_INSTALL_TEMP_DIR="$temp_dir"
    INGEST_INSTALL_NEW_BIN="$COLLECTOR_BIN.new"
    trap 'rm -rf "$INGEST_INSTALL_TEMP_DIR"; rm -f "$INGEST_INSTALL_NEW_BIN"' EXIT

    print_info "Installing OpenTelemetry Collector v$COLLECTOR_VERSION..."
    if ! curl -fsSL --retry 3 --retry-delay 2 --connect-timeout 10 --max-time 300 \
        "$download_url" -o "$archive_path"; then
        print_error "Failed to download OpenTelemetry Collector from $download_url"
        exit 1
    fi

    if command -v sha256sum > /dev/null 2>&1; then
        actual_hash=$(sha256sum "$archive_path" | awk '{print $1}')
    elif command -v shasum > /dev/null 2>&1; then
        actual_hash=$(shasum -a 256 "$archive_path" | awk '{print $1}')
    else
        print_error "Cannot verify download: sha256sum or shasum is required"
        exit 1
    fi

    if [ "$actual_hash" != "$expected_hash" ]; then
        print_error "OpenTelemetry Collector checksum verification failed"
        exit 1
    fi

    tar -xzf "$archive_path" -C "$temp_dir"
    extracted_bin=$(find "$temp_dir" -type f -name otelcol -print -quit)
    if [ -z "$extracted_bin" ]; then
        print_error "OpenTelemetry Collector executable not found in downloaded archive"
        exit 1
    fi

    mkdir -p "$COLLECTOR_DIR"
    cp "$extracted_bin" "$COLLECTOR_BIN.new"
    chmod 755 "$COLLECTOR_BIN.new"
    mv "$COLLECTOR_BIN.new" "$COLLECTOR_BIN"
    rm -rf "$temp_dir"
    trap - EXIT
    unset INGEST_INSTALL_TEMP_DIR INGEST_INSTALL_NEW_BIN
}

start_collector() {
    if is_running; then
        PID=$(cat "$PID_FILE")
        print_warning "OpenTelemetry Collector is already running (PID: $PID)"
        return 0
    fi

    if [ ! -f "$CONFIG_FILE" ]; then
        print_error "Configuration file not found: $CONFIG_FILE"
        print_error "Please run setup first"
        exit 1
    fi

    install_collector

    if ! "$COLLECTOR_BIN" validate --config "$CONFIG_FILE" > /dev/null; then
        print_error "OpenTelemetry Collector configuration validation failed"
        exit 1
    fi

    nohup "$COLLECTOR_BIN" --config "$CONFIG_FILE" > "$LOG_FILE" 2>&1 &
    PID=$!
    echo "$PID" > "$PID_FILE"

    sleep 2
    if ps -p "$PID" > /dev/null 2>&1; then
        print_info "✓ OpenTelemetry Collector started successfully (PID: $PID)"
        print_info "View logs:     tail -f $LOG_FILE"
        print_info "Check status:  ps -p \$(cat $PID_FILE) -o pid,ppid,user,%cpu,%mem,etime,command"
        print_info "Stop:          kill \$(cat $PID_FILE) && rm -f $PID_FILE"
    else
        print_error "✗ OpenTelemetry Collector failed to start. Check logs: cat $LOG_FILE"
        rm -f "$PID_FILE"
        exit 1
    fi
}

restart_collector() {
    stop_collector
    sleep 2
    start_collector
}

yaml_escape() {
    printf '%s' "$1" | sed "s/'/''/g"
}

setup_collector() {
    local ingestor_host="$1"
    local stream_name="$2"
    local api_key="$3"
    local tenant_id="${4:-}"
    local ingestor_scheme="https"
    local default_port="443"
    local port
    local endpoint_yaml
    local api_key_yaml
    local stream_name_yaml
    local tenant_id_yaml
    local host_name_yaml
    local tenant_header=""
    local scrapers
    local bracketed_host_pattern='^(\[[^]]+\])(:([0-9]+))?$'
    local temp_config

    if [ -z "$ingestor_host" ] || [ -z "$stream_name" ] || [ -z "$api_key" ]; then
        print_error "Invalid setup parameters"
        print_error "Expected format: $0 <host[:port]> <stream> <api_key> [tenant_id]"
        exit 1
    fi

    print_parseable_banner

    if [[ "$ingestor_host" =~ ^[Hh][Tt][Tt][Pp][Ss]:// ]]; then
        ingestor_host="${ingestor_host#*://}"
        ingestor_scheme="https"
        default_port="443"
    elif [[ "$ingestor_host" =~ ^[Hh][Tt][Tt][Pp]:// ]]; then
        ingestor_host="${ingestor_host#*://}"
        ingestor_scheme="http"
        default_port="80"
    fi
    ingestor_host="${ingestor_host%%/*}"

    if [[ "$ingestor_host" =~ $bracketed_host_pattern ]]; then
        port="${BASH_REMATCH[3]:-$default_port}"
        ingestor_host="${BASH_REMATCH[1]}"
    elif [[ "$ingestor_host" == *:*:* ]]; then
        print_error "IPv6 hosts must be enclosed in brackets"
        exit 1
    elif [[ "$ingestor_host" == *:* ]]; then
        port="${ingestor_host##*:}"
        ingestor_host="${ingestor_host%:*}"
    else
        port="$default_port"
    fi

    if [ -z "$ingestor_host" ]; then
        print_error "Invalid host"
        exit 1
    fi

    if ! [[ "$port" =~ ^[0-9]+$ ]] || [ "$port" -lt 1 ] || [ "$port" -gt 65535 ]; then
        print_error "Invalid port: $port"
        print_error "Port must be a number between 1 and 65535"
        exit 1
    fi

    install_collector

    endpoint_yaml=$(yaml_escape "${ingestor_scheme}://${ingestor_host}:${port}")
    api_key_yaml=$(yaml_escape "$api_key")
    stream_name_yaml=$(yaml_escape "$stream_name")
    tenant_id_yaml=$(yaml_escape "$tenant_id")
    host_name_yaml=$(yaml_escape "$(hostname)")

    if [ -n "$tenant_id" ]; then
        tenant_header="      X-P-Tenant: '$tenant_id_yaml'"
    fi

    scrapers=$(cat <<'EOF'
      cpu:
      disk:
      filesystem:
      load:
      memory:
      network:
      paging:
      processes:
      system:
EOF
)

    temp_config=$(mktemp "${CONFIG_FILE}.tmp.XXXXXX")
    INGEST_TEMP_CONFIG="$temp_config"
    trap 'rm -f "$INGEST_TEMP_CONFIG"' EXIT
    chmod 600 "$temp_config"
    cat > "$temp_config" << EOF
receivers:
  host_metrics:
    collection_interval: 2s
    scrapers:
$scrapers

processors:
  resource:
    attributes:
      - key: host.name
        value: '$host_name_yaml'
        action: upsert
  batch:
    timeout: 1s

exporters:
  otlp_http/parseable:
    endpoint: '$endpoint_yaml'
    encoding: json
    compression: none
    headers:
      X-API-Key: '$api_key_yaml'
      X-P-Stream: '$stream_name_yaml'
${tenant_header}

service:
  telemetry:
    metrics:
      level: none
  pipelines:
    metrics:
      receivers: [host_metrics]
      processors: [resource, batch]
      exporters: [otlp_http/parseable]
EOF

    if ! "$COLLECTOR_BIN" validate --config "$temp_config" > /dev/null; then
        print_error "OpenTelemetry Collector configuration validation failed"
        exit 1
    fi

    mv -f "$temp_config" "$CONFIG_FILE"
    trap - EXIT
    unset INGEST_TEMP_CONFIG

    if is_running; then
        print_info "Restarting OpenTelemetry Collector to apply updated configuration..."
        stop_collector
        sleep 2
    fi

    echo ""
    start_collector
    print_setup_complete "$stream_name"
}

case "${1:-}" in
    stop)
        stop_collector
        ;;
    restart)
        restart_collector
        ;;
    start)
        start_collector
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    -h|--help|help)
        echo "OpenTelemetry Collector Host Metrics Setup and Management Script"
        echo ""
        echo "Usage:"
        echo "  Setup and start:"
        echo "    $0 <host[:port]> <stream> <api_key> [tenant_id]"
        echo ""
        echo "  Management commands:"
        echo "    $0 start    - Start OpenTelemetry Collector"
        echo "    $0 stop     - Stop OpenTelemetry Collector"
        echo "    $0 restart  - Restart OpenTelemetry Collector"
        echo "    $0 status   - Show OpenTelemetry Collector status"
        echo "    $0 logs     - Show OpenTelemetry Collector logs"
        echo ""
        echo "Example:"
        echo "  $0 https://example.parseable.com:443 node-metrics px_api_key"
        echo "  $0 http://localhost:8000 node-metrics px_api_key tenant-id"
        ;;
    *)
        if [ $# -lt 3 ] || [ $# -gt 4 ]; then
            print_error "Usage: $0 <host[:port]> <stream> <api_key> [tenant_id]"
            print_error "   Or: $0 [start|stop|restart|status|logs|help]"
            exit 1
        fi
        setup_collector "$1" "$2" "$3" "${4:-}"
        ;;
esac
