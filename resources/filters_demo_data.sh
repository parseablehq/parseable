#!/usr/bin/env bash

# Configuration with validation
P_URL=${P_URL:-"http://localhost:8000"}
P_USERNAME=${P_USERNAME:-"admin"}
P_PASSWORD=${P_PASSWORD:-"admin"}
P_STREAM=${P_STREAM:-"demodata"}

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
    echo
fi

# Pre-compute auth header
AUTH_HEADER="Authorization: Basic $(echo -n "$P_USERNAME:$P_PASSWORD" | base64)"

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

# Test connection before creating filters
if [[ "$SILENT" != "true" ]]; then
    print_info "Testing connectivity..."
    if curl_with_retry "$P_URL" "GET" "" "text/html" 1 5; then
        print_info "Basic connectivity OK"
    else
        print_error "Cannot connect to $P_URL - check if server is running"
        exit 1
    fi
fi

# Create comprehensive SQL filters (10 filters)
create_sql_filters() {
    print_info "Creating 10 SQL filters..."
    
    sql_filters=(
        "error_logs|Monitor all ERROR and FATAL severity events|SELECT * FROM $P_STREAM WHERE severity_text IN ('ERROR', 'FATAL') ORDER BY time_unix_nano DESC LIMIT 100"
        "high_response_time|Identify requests with extended response times|SELECT \"service.name\", \"url.path\", body FROM $P_STREAM WHERE body LIKE '%duration%' ORDER BY time_unix_nano DESC LIMIT 50"
        "service_health_summary|Service health metrics by severity|SELECT \"service.name\", severity_text, COUNT(*) as count FROM $P_STREAM GROUP BY \"service.name\", severity_text ORDER BY count DESC"
        "api_endpoint_performance|API endpoint request patterns|SELECT \"url.path\", COUNT(*) as request_count, \"service.name\" FROM $P_STREAM GROUP BY \"url.path\", \"service.name\" ORDER BY request_count DESC LIMIT 20"
        "authentication_failures|Monitor auth-related warnings and errors|SELECT * FROM $P_STREAM WHERE \"url.path\" LIKE '%login%' AND severity_text IN ('WARN', 'ERROR') ORDER BY time_unix_nano DESC LIMIT 100"
        "upstream_cluster_analysis|Request distribution across clusters|SELECT \"upstream.cluster\", COUNT(*) as request_count, \"service.name\" FROM $P_STREAM GROUP BY \"upstream.cluster\", \"service.name\" ORDER BY request_count DESC"
        "trace_analysis|Multi-span traces for distributed tracking|SELECT trace_id, COUNT(*) as span_count, \"service.name\" FROM $P_STREAM GROUP BY trace_id, \"service.name\" HAVING span_count > 1 ORDER BY span_count DESC LIMIT 10"
        "user_agent_distribution|Client types and user agent patterns|SELECT \"user_agent.original\", COUNT(*) as usage_count FROM $P_STREAM GROUP BY \"user_agent.original\" ORDER BY usage_count DESC LIMIT 15"
        "source_address_analysis|Request distribution by source IP|SELECT \"source.address\", COUNT(*) as request_count, COUNT(DISTINCT \"service.name\") as services_accessed FROM $P_STREAM GROUP BY \"source.address\" ORDER BY request_count DESC LIMIT 20"
        "severity_timeline|Severity trends over time|SELECT \"severity_text\", COUNT(*) as count, \"service.name\" FROM $P_STREAM GROUP BY \"severity_text\", \"service.name\" ORDER BY count DESC"
    )
    
    sql_success_count=0
    filter_number=1
    
    for filter_config in "${sql_filters[@]}"; do
        IFS='|' read -r name description query <<< "$filter_config"
        
        [[ "$SILENT" != "true" ]] && echo "Creating SQL filter $filter_number/10: $name"
        
        # Escape quotes for JSON
        escaped_query=$(echo "$query" | sed 's/"/\\"/g')
        escaped_desc=$(echo "$description" | sed 's/"/\\"/g')
        
        json="{\"stream_name\":\"sql\",\"filter_name\":\"$name\",\"filter_description\":\"$escaped_desc\",\"query\":{\"filter_type\":\"sql\",\"filter_query\":\"$escaped_query\"},\"time_filter\":null}"
        
        # Add timeout and better error handling
        if curl_with_retry "$P_URL/api/v1/filters" "POST" "$json" "application/json" 3 10; then
            [[ "$SILENT" != "true" ]] && echo "✓ SQL Filter: $name"
            sql_success_count=$((sql_success_count + 1))
        else
            [[ "$SILENT" != "true" ]] && echo "✗ Failed after retries: $name"
        fi
        
        # Small delay between requests to avoid overwhelming server
        sleep 0.5
        filter_number=$((filter_number + 1))
    done
    
    [[ "$SILENT" != "true" ]] && print_success "Created $sql_success_count/10 SQL filters"
    
    # Wait a bit before creating saved filters
    [[ "$SILENT" != "true" ]] && echo "Waiting 3 seconds before creating saved filters..."
    sleep 3
}

# Create comprehensive saved filters (10 filters)
create_saved_filters() {
    print_info "Creating 10 saved filters..."
    
    saved_filters=(
        "service_errors|Monitor service errors and failures|SELECT * FROM $P_STREAM WHERE severity_text IN ('ERROR', 'FATAL') LIMIT 500|Ingestion Time,Data,service.name,severity_text,url.path|service.name"
        "auth_security_events|Authentication and authorization monitoring|SELECT * FROM $P_STREAM WHERE url.path LIKE '%login%' AND severity_text IN ('WARN', 'ERROR', 'FATAL') LIMIT 500|Ingestion Time,Data,service.name,severity_text,source.address,user_agent.original|severity_text"
        "high_latency_requests|High response time requests|SELECT * FROM $P_STREAM WHERE body LIKE '%duration%' LIMIT 500|Ingestion Time,Data,service.name,url.path,upstream.cluster,body|service.name"
        "upstream_cluster_health|Upstream cluster performance|SELECT * FROM $P_STREAM WHERE upstream.cluster IS NOT NULL LIMIT 500|Ingestion Time,Data,upstream.cluster,service.name,severity_text,destination.address|upstream.cluster"
        "api_endpoint_monitoring|API endpoint usage patterns|SELECT * FROM $P_STREAM WHERE url.path IS NOT NULL LIMIT 500|Ingestion Time,Data,url.path,service.name,severity_text,source.address|url.path"
        "trace_correlation_view|Correlated traces for distributed tracking|SELECT * FROM $P_STREAM WHERE trace_id IS NOT NULL AND span_id IS NOT NULL LIMIT 500|Ingestion Time,Data,trace_id,span_id,service.name,url.path|trace_id"
        "user_agent_analysis|Client types and patterns|SELECT * FROM $P_STREAM WHERE user_agent.original IS NOT NULL LIMIT 500|Ingestion Time,Data,user_agent.original,source.address,url.path,service.name|user_agent.original"
        "network_monitoring|Network traffic and server interactions|SELECT * FROM $P_STREAM WHERE source.address IS NOT NULL LIMIT 500|Ingestion Time,Data,source.address,destination.address,service.name,severity_text,url.path|source.address"
        "service_overview|Comprehensive service activity view|SELECT * FROM $P_STREAM LIMIT 500|Ingestion Time,Data,service.name,url.path,source.address,destination.address,upstream.cluster|service.name"
        "recent_activity|Most recent system activity|SELECT * FROM $P_STREAM ORDER BY time_unix_nano DESC LIMIT 500|Ingestion Time,Data,service.name,severity_text,url.path,source.address|severity_text"
    )
    
    saved_success_count=0
    filter_number=1
    
    for filter_config in "${saved_filters[@]}"; do
        IFS='|' read -r name description query visible_columns group_by <<< "$filter_config"
        
        [[ "$SILENT" != "true" ]] && echo "Creating saved filter $filter_number/10: $name"
        
        # Escape quotes
        escaped_query=$(echo "$query" | sed 's/"/\\"/g')
        escaped_desc=$(echo "$description" | sed 's/"/\\"/g')
        
        # Convert visible columns to JSON array
        IFS=',' read -ra col_array <<< "$visible_columns"
        visible_cols_json=""
        for i in "${!col_array[@]}"; do
            [[ $i -gt 0 ]] && visible_cols_json+=","
            visible_cols_json+="\"${col_array[$i]}\""
        done
        
        json="{\"stream_name\":\"$P_STREAM\",\"filter_name\":\"$name\",\"filter_description\":\"$escaped_desc\",\"query\":{\"filter_type\":\"filter\",\"filter_query\":\"$escaped_query\"},\"time_filter\":null,\"tableConfig\":{\"visibleColumns\":[$visible_cols_json],\"pinnedColumns\":[]},\"groupBy\":\"$group_by\"}"
        
        # Add timeout and better error handling for saved filters
        if curl_with_retry "$P_URL/api/v1/filters" "POST" "$json" "application/json" 3 10; then
            [[ "$SILENT" != "true" ]] && echo "✓ Saved Filter: $name"
            saved_success_count=$((saved_success_count + 1))
        else
            [[ "$SILENT" != "true" ]] && echo "✗ Failed after retries: $name"
        fi
        
        # Small delay between requests
        sleep 0.5
        filter_number=$((filter_number + 1))
    done
    
    [[ "$SILENT" != "true" ]] && print_success "Created $saved_success_count/10 saved filters"
}

# Create all filters
create_sql_filters
create_saved_filters

print_success "Filter creation completed successfully!"

# Always exit with success if we get here
exit 0