#!/usr/bin/env bash

# Configuration
P_URL=${P_URL:-"http://localhost:8000"}
P_USERNAME=${P_USERNAME:-"admin"}
P_PASSWORD=${P_PASSWORD:-"admin"}
P_STREAM=${P_STREAM:-"demodata"}

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

# Create SQL filters
create_sql_filters() {
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
    
    for filter_config in "${sql_filters[@]}"; do
        IFS='|' read -r name description query <<< "$filter_config"
        
        # Escape quotes for JSON
        escaped_query=$(echo "$query" | sed 's/"/\\"/g')
        escaped_desc=$(echo "$description" | sed 's/"/\\"/g')
        
        json="{\"stream_name\":\"sql\",\"filter_name\":\"$name\",\"filter_description\":\"$escaped_desc\",\"query\":{\"filter_type\":\"sql\",\"filter_query\":\"$escaped_query\"},\"time_filter\":null}"
        
        if curl_with_retry "$P_URL/api/v1/filters" "POST" "$json" "application/json" 3 10; then
            sql_success_count=$((sql_success_count + 1))
        fi
        
        sleep 0.5
    done
    
    sleep 3
}

# Create saved filters
create_saved_filters() {
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
    
    for filter_config in "${saved_filters[@]}"; do
        IFS='|' read -r name description query visible_columns group_by <<< "$filter_config"
        
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
        
        if curl_with_retry "$P_URL/api/v1/filters" "POST" "$json" "application/json" 3 10; then
            saved_success_count=$((saved_success_count + 1))
        fi
        
        sleep 0.5
    done
}

# Create all filters
create_sql_filters
create_saved_filters

exit 0