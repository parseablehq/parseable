#!/usr/bin/env bash

# Configuration with validation
P_URL=${P_URL:-"http://localhost:8000"}
P_USERNAME=${P_USERNAME:-"admin"}
P_PASSWORD=${P_PASSWORD:-"admin"}
P_STREAM=${P_STREAM:-"demodata"}
TARGET_RECORDS=10000
BATCH_SIZE=1000  # Back to 1000 for maximum throughput

# Silent mode handling
SILENT=${SILENT:-false}
for arg in "$@"; do
    case $arg in
        --silent)
            SILENT=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [--silent]"
            echo "  --silent    Run in silent mode"
            exit 0
            ;;
    esac
done

# Only show config if not silent
if [[ "$SILENT" != "true" ]]; then
    echo "Configuration:"
    echo "  URL: $P_URL"
    echo "  Username: $P_USERNAME"
    echo "  Stream: $P_STREAM"
    echo "  Target Records: $TARGET_RECORDS"
    echo "  Batch Size: $BATCH_SIZE"
    echo
fi

# Performance tracking
START_TIME=$(date +%s)
RECORDS_SENT=0

# Common curl function with retry logic
curl_with_retry() {
    local url="$1"
    local method="$2"
    local data="$3"
    local content_type="${4:-application/json}"
    local max_retries="${5:-3}"
    local base_timeout="${6:-15}"
    local retry_count=0
    
    # Set timeout based on retry attempt: 10s, 20s, 30s
    local max_time=$((10 + (retry_count * 10)))
    local connect_timeout=5
    
    # Create temp file if data is provided
    if [[ -n "$data" ]]; then
        temp_file=$(mktemp)
        if [[ $? -ne 0 ]]; then
            print_error "Failed to create temporary file"
            return 1
        fi
        echo "$data" > "$temp_file"
    fi
    
    while [[ $retry_count -lt $max_retries ]]; do
        # Current timeout: 10s, 20s, 30s for attempts 1, 2, 3
        max_time=$((10 + (retry_count * 10)))
        
        local curl_cmd="curl -s -w \"\n%{http_code}\" --max-time $max_time --connect-timeout $connect_timeout"
        
        # Add headers
        curl_cmd+=" -H \"Content-Type: $content_type\""
        curl_cmd+=" -H \"$AUTH_HEADER\""
        
        # Add stream header for ingestion requests
        if [[ "$url" == *"/ingest"* ]]; then
            curl_cmd+=" -H \"X-P-STREAM: $P_STREAM\""
        fi
        
        # Add method and data
        if [[ "$method" == "POST" ]]; then
            curl_cmd+=" -X POST"
            if [[ -n "$temp_file" ]]; then
                curl_cmd+=" --data-binary \"@$temp_file\""
            elif [[ -n "$data" ]]; then
                curl_cmd+=" -d \"$data\""
            fi
        fi
        
        # Add URL
        curl_cmd+=" \"$url\""
        
        # Execute curl
        local response
        response=$(eval "$curl_cmd" 2>&1)
        local curl_exit_code=$?
        
        # Check curl exit code
        if [[ $curl_exit_code -eq 0 ]]; then
            # Success - extract status code and return
            local status_code
            if [[ -n "$response" ]]; then
                status_code=$(echo "$response" | tail -n1)
                local response_body=$(echo "$response" | sed '$d')
                
                # Clean up temp file
                [[ -n "$temp_file" ]] && rm -f "$temp_file"
                
                if [[ "$status_code" == "200" || "$status_code" == "201" ]]; then
                    return 0
                else
                    print_error "HTTP $status_code: $response_body"
                    return 1
                fi
            else
                print_error "No response from server"
                return 1
            fi
        elif [[ $curl_exit_code -eq 28 ]]; then
            # Timeout - retry immediately with next timeout level
            retry_count=$((retry_count + 1))
            
            if [[ "$SILENT" != "true" && -n "$data" ]]; then
                echo "Timeout (${#data} chars) - retry $retry_count with $((10 + (retry_count * 10)))s timeout"
            elif [[ "$SILENT" != "true" ]]; then
                echo "Timeout - retry $retry_count with $((10 + (retry_count * 10)))s timeout"
            fi
            
            # Brief pause before retry
            sleep 1
        else
            # Other error - break and report
            break
        fi
    done
    
    # Clean up temp file on failure
    [[ -n "$temp_file" ]] && rm -f "$temp_file"
    
    # Final error reporting
    print_error "curl failed with exit code $curl_exit_code after $retry_count retries"
    if [[ -n "$data" ]]; then
        print_error "Data size: ${#data} characters, Final timeout: ${max_time}s"
    fi
    [[ "$SILENT" != "true" ]] && print_error "Response: $response"
    
    return 1
}

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() { [[ "$SILENT" != "true" ]] && echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { [[ "$SILENT" != "true" ]] && echo -e "${GREEN}[SUCCESS]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

# Pre-compute ALL static data once (no random generation in loop)
[[ "$SILENT" != "true" ]] && echo "Initializing static data..."
TRACE_IDS=()
SPAN_IDS=()
IP_ADDRESSES=()
TIMESTAMPS=()
UNIX_NANOS=()

# Generate 100 of each for cycling through
for i in {1..100}; do
    TRACE_IDS+=("$(printf '%032x' $((RANDOM * RANDOM)))")
    SPAN_IDS+=("$(printf '%016x' $((RANDOM * RANDOM)))")
    IP_ADDRESSES+=("192.168.$((RANDOM % 256)).$((RANDOM % 256))")
    TIMESTAMPS+=("$(date -u +%Y-%m-%dT%H:%M:%S.%03dZ -d "+$((RANDOM % 3600)) seconds")")
    UNIX_NANOS+=("$(date +%s)$(printf '%09d' $((RANDOM % 1000000000)))")
done

# Static arrays for ultra-fast selection
METHODS=("GET" "GET" "GET" "GET" "POST" "PUT")
STATUS_CODES=(200 200 200 201 400 500)
SERVICES=("frontend" "api" "auth" "cart" "payment")
ENDPOINTS=("/products" "/cart" "/login" "/checkout" "/search")
USER_AGENTS=("curl/7.88.1" "python-requests/2.32.3" "Mozilla/5.0")
CLUSTERS=("web" "api" "db")

# Pre-compute auth header
AUTH_HEADER="Authorization: Basic $(echo -n "$P_USERNAME:$P_PASSWORD" | base64)"

[[ "$SILENT" != "true" ]] && echo "Static data initialized. Starting generation..."

# Ultra-fast log generation using array cycling
generate_batch() {
    batch_size=$1
    
    # Validate input
    if [[ -z "$batch_size" || "$batch_size" -eq 0 ]]; then
        print_error "generate_batch called with invalid batch_size: $batch_size"
        return 1
    fi
    
    # Check if arrays are initialized
    if [[ ${#TRACE_IDS[@]} -eq 0 ]]; then
        print_error "Static data arrays not initialized"
        return 1
    fi
    
    batch_data='['
    
    for ((i=0; i<batch_size; i++)); do
        # Use modulo for cycling through pre-computed arrays
        idx=$((i % 100))
        method_idx=$((i % 6))
        status_idx=$((i % 6))
        service_idx=$((i % 5))
        endpoint_idx=$((i % 5))
        agent_idx=$((i % 3))
        cluster_idx=$((i % 3))
        
        # Direct array access (no random calls in loop)
        trace_id=${TRACE_IDS[$idx]}
        span_id=${SPAN_IDS[$idx]}
        source_ip=${IP_ADDRESSES[$idx]}
        dest_ip=${IP_ADDRESSES[$(((idx + 1) % 100))]}
        timestamp=${TIMESTAMPS[$idx]}
        unix_nano=${UNIX_NANOS[$idx]}
        method=${METHODS[$method_idx]}
        status=${STATUS_CODES[$status_idx]}
        service=${SERVICES[$service_idx]}
        endpoint=${ENDPOINTS[$endpoint_idx]}
        user_agent=${USER_AGENTS[$agent_idx]}
        cluster=${CLUSTERS[$cluster_idx]}
        
        # Severity based on status
        severity_num=10
        severity_text="INFO"
        if [[ $status -ge 400 && $status -lt 500 ]]; then
            severity_num=14
            severity_text="WARN"
        elif [[ $status -ge 500 ]]; then
            severity_num=18
            severity_text="ERROR"
        fi
        
        # Escape user agent for JSON (simple approach)
        escaped_user_agent=$(echo "$user_agent" | sed 's/"/\\"/g' | sed "s/'/\\'/g")
        
        # Single-line JSON generation with proper field structure
        batch_data+="{\"body\":\"[$timestamp] $method $endpoint HTTP/1.1 $status - bytes:$((500 + i % 1000)) duration:$((10 + i % 90))ms\",\"time_unix_nano\":\"$unix_nano\",\"observed_time_unix_nano\":\"$unix_nano\",\"trace_id\":\"$trace_id\",\"span_id\":\"$span_id\",\"flags\":0,\"severity_number\":$severity_num,\"severity_text\":\"$severity_text\",\"service.name\":\"$service\",\"source.address\":\"$source_ip\",\"destination.address\":\"$dest_ip\",\"server.address\":\"$dest_ip\",\"url.path\":\"$endpoint\",\"url.full\":\"http://$service:8080$endpoint\",\"upstream.cluster\":\"$cluster\",\"user_agent.original\":\"$escaped_user_agent\",\"event.name\":\"proxy.access\"}"
        
        # Add comma except for last item
        [[ $i -lt $((batch_size - 1)) ]] && batch_data+=','
    done
    
    batch_data+=']'
    
    # Validate JSON structure
    if [[ ${#batch_data} -lt 10 ]]; then
        print_error "Generated batch too small: ${#batch_data} characters"
        return 1
    fi
    
    echo "$batch_data"
}

# Send batch with minimal overhead
send_batch() {
    local data="$1"
    
    # Validate input
    if [[ -z "$data" ]]; then
        print_error "send_batch called with empty data"
        return 1
    fi
    
    # Use common curl function with retry logic
    curl_with_retry "$P_URL/api/v1/ingest" "POST" "$data" "application/json" 3 15
}

# Main execution loop - optimized for speed
[[ "$SILENT" != "true" ]] && echo "Starting batch generation and sending..."

# Test connection and basic functionality first
if [[ "$SILENT" != "true" ]]; then
    print_info "Testing basic connectivity..."
    if curl_with_retry "$P_URL" "GET" "" "text/html" 1 5; then
        print_info "Basic connectivity OK"
    else
        print_error "Cannot connect to $P_URL - check if server is running"
        exit 1
    fi
fi

# Generate a small test batch first to validate everything works
if [[ "$SILENT" != "true" ]]; then
    print_info "Testing batch generation..."
    test_batch=$(generate_batch 1)
    if [[ -z "$test_batch" || ${#test_batch} -lt 50 ]]; then
        print_error "Batch generation failed - generated: ${#test_batch} characters"
        exit 1
    fi
    print_info "Batch generation OK (${#test_batch} characters)"
    
    print_info "Testing first ingestion..."
    if ! send_batch "$test_batch"; then
        print_error "Test ingestion failed - check credentials and stream name"
        exit 1
    fi
    print_info "Test ingestion successful"
    RECORDS_SENT=1
fi

while [[ $RECORDS_SENT -lt $TARGET_RECORDS ]]; do
    # Calculate remaining records
    remaining=$((TARGET_RECORDS - RECORDS_SENT))
    current_batch_size=$((remaining > BATCH_SIZE ? BATCH_SIZE : remaining))
    
    # Progress tracking (only if not silent)
    if [[ "$SILENT" != "true" ]]; then
        progress=$((RECORDS_SENT * 100 / TARGET_RECORDS))
        elapsed=$(($(date +%s) - START_TIME))
        rate=$((RECORDS_SENT / (elapsed == 0 ? 1 : elapsed)))
        echo "Progress: ${progress}% (${RECORDS_SENT}/${TARGET_RECORDS}) | Rate: ${rate} records/sec | Elapsed: ${elapsed}s"
    fi
    
    # Generate and send batch with error checking
    if [[ "$SILENT" != "true" ]]; then
        echo "Generating batch of $current_batch_size records..."
    fi
    
    batch_data=$(generate_batch $current_batch_size)
    
    if [[ -z "$batch_data" ]]; then
        print_error "Failed to generate batch data"
        exit 1
    fi
    
    if [[ "$SILENT" != "true" ]]; then
        echo "Sending batch (${#batch_data} characters)..."
    fi
    
    if send_batch "$batch_data"; then
        RECORDS_SENT=$((RECORDS_SENT + current_batch_size))
        [[ "$SILENT" != "true" ]] && echo "✓ Batch sent successfully"
    else
        print_error "Failed to send batch at record $RECORDS_SENT"
        exit 1
    fi
    
    # Small delay to prevent overwhelming the server
    sleep 0.1
done

# Final statistics
TOTAL_TIME=$(($(date +%s) - START_TIME))
FINAL_RATE=$((TARGET_RECORDS / (TOTAL_TIME == 0 ? 1 : TOTAL_TIME)))

if [[ "$SILENT" != "true" ]]; then
    echo -e "\n"
    print_success "Completed $TARGET_RECORDS records in ${TOTAL_TIME} seconds"
    print_success "Average rate: ${FINAL_RATE} records/second"
    print_success "Total batches: $((TARGET_RECORDS / BATCH_SIZE))"
fi

# Performance tips shown (only if not silent)
if [[ "$SILENT" != "true" ]]; then
    echo
    print_info "Performance optimizations used:"
    echo "  • Pre-computed all random data (no runtime generation)"
    echo "  • Large batch sizes (fewer HTTP requests)"
    echo "  • Array cycling instead of random selection"
    echo "  • Minimal JSON formatting"
    echo "  • curl connection keep-alive"
    echo "  • Single-process execution (no worker overhead)"
fi

# Always exit with success if we get here
exit 0