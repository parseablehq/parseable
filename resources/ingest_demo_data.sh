#!/usr/bin/env bash

# Configuration
P_URL=${P_URL:-"http://localhost:8000"}
P_USERNAME=${P_USERNAME:-"admin"}
P_PASSWORD=${P_PASSWORD:-"admin"}
P_STREAM=${P_STREAM:-"demodata"}
TARGET_RECORDS=10000
BATCH_SIZE=1000

# Pre-compute auth header
AUTH_HEADER="Authorization: Basic $(echo -n "$P_USERNAME:$P_PASSWORD" | base64)"

# Common curl function with retry logic
curl_with_retry() {
    local url="$1"
    local method="$2"
    local data="$3"
    local content_type="${4:-application/json}"
    local max_retries="${5:-3}"
    local retry_count=0
    
    # Create temp file if data is provided
    if [[ -n "$data" ]]; then
        temp_file=$(mktemp)
        if [[ $? -ne 0 ]]; then
            return 1
        fi
        echo "$data" > "$temp_file"
    fi
    
    while [[ $retry_count -lt $max_retries ]]; do
        local max_time=$((10 + (retry_count * 10)))
        local connect_timeout=5
        
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
            local status_code
            if [[ -n "$response" ]]; then
                status_code=$(echo "$response" | tail -n1)
                
                # Clean up temp file
                [[ -n "$temp_file" ]] && rm -f "$temp_file"
                
                if [[ "$status_code" == "200" || "$status_code" == "201" ]]; then
                    return 0
                else
                    return 1
                fi
            else
                return 1
            fi
        elif [[ $curl_exit_code -eq 28 ]]; then
            # Timeout - retry
            retry_count=$((retry_count + 1))
            sleep 1
        else
            break
        fi
    done
    
    # Clean up temp file on failure
    [[ -n "$temp_file" ]] && rm -f "$temp_file"
    
    return 1
}

# Pre-compute static data
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

# Static arrays
METHODS=("GET" "GET" "GET" "GET" "POST" "PUT")
STATUS_CODES=(200 200 200 201 400 500)
SERVICES=("frontend" "api" "auth" "cart" "payment")
ENDPOINTS=("/products" "/cart" "/login" "/checkout" "/search")
USER_AGENTS=("curl/7.88.1" "python-requests/2.32.3" "Mozilla/5.0")
CLUSTERS=("web" "api" "db")

# Generate batch data
generate_batch() {
    batch_size=$1
    
    if [[ -z "$batch_size" || "$batch_size" -eq 0 ]]; then
        return 1
    fi
    
    if [[ ${#TRACE_IDS[@]} -eq 0 ]]; then
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
        
        # Direct array access
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
        
        # Escape user agent for JSON
        escaped_user_agent=$(echo "$user_agent" | sed 's/"/\\"/g' | sed "s/'/\\'/g")
        
        # Generate JSON record
        batch_data+="{\"body\":\"[$timestamp] $method $endpoint HTTP/1.1 $status - bytes:$((500 + i % 1000)) duration:$((10 + i % 90))ms\",\"time_unix_nano\":\"$unix_nano\",\"observed_time_unix_nano\":\"$unix_nano\",\"trace_id\":\"$trace_id\",\"span_id\":\"$span_id\",\"flags\":0,\"severity_number\":$severity_num,\"severity_text\":\"$severity_text\",\"service.name\":\"$service\",\"source.address\":\"$source_ip\",\"destination.address\":\"$dest_ip\",\"server.address\":\"$dest_ip\",\"url.path\":\"$endpoint\",\"url.full\":\"http://$service:8080$endpoint\",\"upstream.cluster\":\"$cluster\",\"user_agent.original\":\"$escaped_user_agent\",\"event.name\":\"proxy.access\"}"
        
        [[ $i -lt $((batch_size - 1)) ]] && batch_data+=','
    done
    
    batch_data+=']'
    
    if [[ ${#batch_data} -lt 10 ]]; then
        return 1
    fi
    
    echo "$batch_data"
}

# Send batch
send_batch() {
    local data="$1"
    
    if [[ -z "$data" ]]; then
        return 1
    fi
    
    curl_with_retry "$P_URL/api/v1/ingest" "POST" "$data" "application/json" 3 15
}

# Main execution
START_TIME=$(date +%s)
RECORDS_SENT=0

while [[ $RECORDS_SENT -lt $TARGET_RECORDS ]]; do
    remaining=$((TARGET_RECORDS - RECORDS_SENT))
    current_batch_size=$((remaining > BATCH_SIZE ? BATCH_SIZE : remaining))
    
    # Progress tracking
    progress=$((RECORDS_SENT * 100 / TARGET_RECORDS))
    elapsed=$(($(date +%s) - START_TIME))
    rate=$((RECORDS_SENT / (elapsed == 0 ? 1 : elapsed)))
    
    # Generate and send batch
    batch_data=$(generate_batch $current_batch_size)
    
    if [[ -z "$batch_data" ]]; then
        exit 1
    fi
    
    if send_batch "$batch_data"; then
        RECORDS_SENT=$((RECORDS_SENT + current_batch_size))
    else
        exit 1
    fi
    
    sleep 0.1
done

# Final statistics
TOTAL_TIME=$(($(date +%s) - START_TIME))
FINAL_RATE=$((TARGET_RECORDS / (TOTAL_TIME == 0 ? 1 : TOTAL_TIME)))

exit 0