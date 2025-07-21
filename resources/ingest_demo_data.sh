#!/usr/bin/env bash

# Configuration
P_URL=${P_URL:-"http://localhost:8000"}
P_USERNAME=${P_USERNAME:-"admin"}
P_PASSWORD=${P_PASSWORD:-"admin"}
P_STREAM=${P_STREAM:-"demodata"}
ACTION=${ACTION:-"ingest"}
TARGET_RECORDS=5000
BATCH_SIZE=500

# Pre-compute auth header
AUTH_HEADER="Authorization: Basic $(echo -n "$P_USERNAME:$P_PASSWORD" | base64)"

# Logging functions
log_error() {
    echo "$@" >&2
}

# Common curl function with retry logic
curl_with_retry() {
    local url="$1"
    local method="$2"
    local data="$3"
    local content_type="${4:-application/json}"
    local max_retries="${5:-3}"
    local data_file="$6"
    local retry_count=0
    
    local temp_file=""
    # Create temp file if data is provided (either as string or file)
    if [[ -n "$data_file" ]]; then
        temp_file="$data_file"
    elif [[ -n "$data" ]]; then
        temp_file=$(mktemp)
        if [[ $? -ne 0 ]]; then
            return 1
        fi
        printf "%s" "$data" > "$temp_file"
    fi
    
    while [[ $retry_count -lt $max_retries ]]; do
        local max_time=$((10 + (retry_count * 10)))
        local connect_timeout=5
        
        local curl_args=(
            -s
            --max-time "$max_time"
            --connect-timeout "$connect_timeout"
            -H "Content-Type: $content_type"
            -H "$AUTH_HEADER"
        )
        
        # Add stream header for ingestion requests
        if [[ "$url" == *"/ingest"* ]]; then
            curl_args+=(-H "X-P-STREAM: $P_STREAM")
        fi
        
        # Add method and data
        if [[ "$method" == "POST" ]]; then
            curl_args+=(-X POST)
            if [[ -n "$temp_file" ]]; then
                curl_args+=(--data-binary "@$temp_file")
            elif [[ -n "$data" ]]; then
                curl_args+=(-d "$data")
            fi
        elif [[ "$method" == "PUT" ]]; then
            curl_args+=(-X PUT)
            if [[ -n "$temp_file" ]]; then
                curl_args+=(--data-binary "@$temp_file")
            elif [[ -n "$data" ]]; then
                curl_args+=(-d "$data")
            fi
        fi
        
        # Add URL
        curl_args+=("$url")
        
        # Create temporary files for response body and stderr
        local response_file
        response_file=$(mktemp) || { log_error "Failed to create temporary file"; return 1; }
        local stderr_file
        stderr_file=$(mktemp) || { log_error "Failed to create temporary file"; rm -f "$response_file"; return 1; }
        
        # Add options to capture status code separately
        curl_args+=("-w" "%{http_code}" "-o" "$response_file")
        
        # Execute curl and capture status code and stderr
        local status_code
        status_code=$(curl "${curl_args[@]}" 2>"$stderr_file" | tr -d '\n')
        local curl_exit_code=$?
        
        # Check curl exit code
        if [[ $curl_exit_code -eq 0 ]]; then
            local response_body
            response_body=$(cat "$response_file" 2>/dev/null)
            
            # Clean up temporary files
            rm -f "$response_file" "$stderr_file"
            
            # Clean up temp data file (only if we created it)
            if [[ -n "$temp_file" && -z "$data_file" ]]; then
                rm -f "$temp_file"
            fi
            
            if [[ "$status_code" =~ ^2[0-9][0-9]$ ]]; then
                echo "$response_body"
                return 0
            else
                log_error "HTTP $status_code: Request failed"
                return 1
            fi
        elif [[ $curl_exit_code -eq 28 ]]; then
            # Timeout - retry
            rm -f "$response_file" "$stderr_file"
            retry_count=$((retry_count + 1))
            sleep 1
        else
            # Other curl error - cleanup and exit
            rm -f "$response_file" "$stderr_file"
            break
        fi
    done
    
    # Clean up temp file on failure (only if we created it)
    if [[ -n "$temp_file" && -z "$data_file" ]]; then
        rm -f "$temp_file"
    fi
    
    return 1
}

# ==================== INGEST FUNCTIONALITY ====================

# Pre-compute static data for ingestion
init_ingest_data() {
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
    METHODS=("GET" "POST" "PUT" "DELETE" "PATCH" "HEAD")
    METHODS_LEN=${#METHODS[@]}
    STATUS_CODES=(200 400 401 500 503)
    STATUS_CODES_LEN=${#STATUS_CODES[@]}
    SERVICES=("frontend" "api" "auth" "cart" "payment")
    SERVICES_LEN=${#SERVICES[@]}
    ENDPOINTS=("/products" "/cart" "/login" "/checkout" "/search")
    ENDPOINTS_LEN=${#ENDPOINTS[@]}
    USER_AGENTS=("curl/7.88.1" "python-requests/2.32.3" "Mozilla/5.0")
    USER_AGENTS_LEN=${#USER_AGENTS[@]}
    CLUSTERS=("web" "api" "db")
    CLUSTERS_LEN=${#CLUSTERS[@]}
}

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
        method_idx=$(( i % METHODS_LEN ))
        status_idx=$((i % STATUS_CODES_LEN))
        service_idx=$((i % SERVICES_LEN))
        endpoint_idx=$((i % ENDPOINTS_LEN))
        agent_idx=$((i % USER_AGENTS_LEN))
        cluster_idx=$((i % CLUSTERS_LEN))
        
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
    
    curl_with_retry "$P_URL/api/v1/ingest" "POST" "$data" "application/json" 3
}

# Main ingest function
run_ingest() {
    echo "Starting data ingestion..."
    init_ingest_data
    
    START_TIME=$(date +%s)
    RECORDS_SENT=0

    while [[ $RECORDS_SENT -lt $TARGET_RECORDS ]]; do
        remaining=$((TARGET_RECORDS - RECORDS_SENT))
        current_batch_size=$((remaining > BATCH_SIZE ? BATCH_SIZE : remaining))
        
        # Progress tracking
        progress=$((RECORDS_SENT * 100 / TARGET_RECORDS))
        elapsed=$(($(date +%s) - START_TIME))
        rate=$((RECORDS_SENT / (elapsed == 0 ? 1 : elapsed)))
        
        echo "Progress: $progress% ($RECORDS_SENT/$TARGET_RECORDS) - Rate: $rate records/sec"
        
        # Generate and send batch
        batch_data=$(generate_batch $current_batch_size)
        
        if [[ -z "$batch_data" ]]; then
            echo "Failed to generate batch data"
            exit 1
        fi
        
        if send_batch "$batch_data"; then
            RECORDS_SENT=$((RECORDS_SENT + current_batch_size))
        else
            echo "Failed to send batch"
            exit 1
        fi
    done

    # Final statistics
    TOTAL_TIME=$(($(date +%s) - START_TIME))
    FINAL_RATE=$((TARGET_RECORDS / (TOTAL_TIME == 0 ? 1 : TOTAL_TIME)))
    
    echo "Ingestion completed: $TARGET_RECORDS records in $TOTAL_TIME seconds (avg: $FINAL_RATE records/sec)"
}

# ==================== FILTERS FUNCTIONALITY ====================

# Create SQL filters
create_sql_filters() {
    echo "Creating SQL filters..."
    
    sql_filters=(
        "error_logs|Monitor all ERROR and FATAL severity events|SELECT * FROM $P_STREAM WHERE severity_text IN ('ERROR', 'FATAL') ORDER BY time_unix_nano DESC LIMIT 100"
        "high_response_time|Identify requests with extended response times|SELECT \"service.name\", \"url.path\", body FROM $P_STREAM WHERE body LIKE '%duration%' ORDER BY time_unix_nano DESC LIMIT 50"
        "service_health_summary|Service health metrics by severity|SELECT \"service.name\", severity_text, COUNT(*) as count FROM $P_STREAM GROUP BY \"service.name\", severity_text ORDER BY count DESC"
        "api_endpoint_performance|API endpoint request patterns|SELECT \"url.path\", COUNT(*) as request_count, \"service.name\" FROM $P_STREAM GROUP BY \"url.path\", \"service.name\" ORDER BY request_count DESC LIMIT 20"
        "authentication_failures|Monitor auth-related warnings and errors|SELECT * FROM $P_STREAM WHERE \"url.path\" LIKE '%login%' AND severity_text IN ('WARN', 'ERROR') ORDER BY time_unix_nano DESC LIMIT 100"
    )
    
    sql_success_count=0
    
    for filter_config in "${sql_filters[@]}"; do
        IFS='|' read -r name description query <<< "$filter_config"
        
        # Escape quotes for JSON
        escaped_query=$(echo "$query" | sed 's/"/\\"/g')
        escaped_desc=$(echo "$description" | sed 's/"/\\"/g')
        
        json="{\"stream_name\":\"sql\",\"filter_name\":\"$name\",\"filter_description\":\"$escaped_desc\",\"query\":{\"filter_type\":\"sql\",\"filter_query\":\"$escaped_query\"},\"time_filter\":null}"
        
        if curl_with_retry "$P_URL/api/v1/filters" "POST" "$json" "application/json" 3; then
            sql_success_count=$((sql_success_count + 1))
            echo "Created SQL filter: $name"
        else
            echo "Failed to create SQL filter: $name"
        fi
    done
    
    echo "Created $sql_success_count SQL filters"
}

# Create saved filters
create_saved_filters() {
    echo "Creating saved filters..."
    
    saved_filters=(
        "service_errors|Monitor service errors and failures|SELECT * FROM $P_STREAM WHERE severity_text IN ('ERROR', 'FATAL') LIMIT 500|Ingestion Time,Data,service.name,severity_text,url.path|service.name"
        "auth_security_events|Authentication and authorization monitoring|SELECT * FROM $P_STREAM WHERE url.path LIKE '%login%' AND severity_text IN ('WARN', 'ERROR', 'FATAL') LIMIT 500|Ingestion Time,Data,service.name,severity_text,source.address,user_agent.original|severity_text"
        "high_latency_requests|High response time requests|SELECT * FROM $P_STREAM WHERE body LIKE '%duration%' LIMIT 500|Ingestion Time,Data,service.name,url.path,upstream.cluster,body|service.name"
        "upstream_cluster_health|Upstream cluster performance|SELECT * FROM $P_STREAM WHERE upstream.cluster IS NOT NULL LIMIT 500|Ingestion Time,Data,upstream.cluster,service.name,severity_text,destination.address|upstream.cluster"
        "api_endpoint_monitoring|API endpoint usage patterns|SELECT * FROM $P_STREAM WHERE url.path IS NOT NULL LIMIT 500|Ingestion Time,Data,url.path,service.name,severity_text,source.address|url.path"
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
        
        if curl_with_retry "$P_URL/api/v1/filters" "POST" "$json" "application/json" 3; then
            saved_success_count=$((saved_success_count + 1))
            echo "Created saved filter: $name"
        else
            echo "Failed to create saved filter: $name"
        fi
    done
    
    echo "Created $saved_success_count saved filters"
}

# Main filters function
run_filters() {
    echo "Starting filter creation..."
    create_sql_filters
    create_saved_filters
    echo "Filter creation completed"
}

# ==================== ALERTS FUNCTIONALITY ====================

# Create webhook target
create_target() {
    echo "Creating webhook target..." >&2
    
    response=$(curl -s -H "Content-Type: application/json" -H "$AUTH_HEADER" -X POST "$P_URL/api/v1/targets" -d @- << EOF
{"type":"webhook","endpoint":"https://webhook.site/8e1f26bd-2f5b-47a2-9d0b-3b3dabb30710","name":"Test Webhook","auth":{"username":"","password":""},"skipTlsCheck":false,"notificationConfig":{"interval":1,"times":1}}
EOF
)
    
    curl_exit_code=$?
    
    if [[ $curl_exit_code -eq 0 && -n "$response" ]]; then
        # Extract target ID from response
        target_id=$(echo "$response" | jq -r '.id // empty')
        if [[ -n "$target_id" ]]; then
            echo "Target created successfully with ID: $target_id" >&2
            echo "$target_id"
            return 0
        else
            echo "Failed to extract target ID from response" >&2
            echo "Response: $response" >&2
            return 1
        fi
    else
        echo "Failed to create target" >&2
        echo "Curl exit code: $curl_exit_code" >&2
        echo "Response: $response" >&2
        return 1
    fi
}

# Create alerts
create_alerts() {
    local target_id="$1"
    
    if [[ -z "$target_id" ]]; then
        echo "Target ID is required to create alerts"
        return 1
    fi
    
    echo "Creating alerts with target ID: $target_id"
    
    # Alert 1: Error Count (severity_number = 18)
    alert1_json="{\"severity\":\"high\",\"title\":\"error count\",\"stream\":\"$P_STREAM\",\"alertType\":\"threshold\",\"aggregates\":{\"aggregateConfig\":[{\"aggregateFunction\":\"count\",\"conditions\":{\"operator\":null,\"conditionConfig\":[{\"column\":\"severity_number\",\"operator\":\"=\",\"value\":\"18\"}]},\"column\":\"severity_number\",\"operator\":\">\",\"value\":1000}]},\"evalConfig\":{\"rollingWindow\":{\"evalStart\":\"5h\",\"evalEnd\":\"now\",\"evalFrequency\":1}},\"targets\":[\"$target_id\"]}"
    
    response1=$(curl_with_retry "$P_URL/api/v1/alerts" "POST" "$alert1_json" "application/json" 3)
    if [[ $? -eq 0 ]]; then
        echo "Alert 1 (Error Count) created successfully"
    else
        echo "Failed to create Alert 1 (Error Count)"
        echo "Response: $response1"
    fi
    
    # Alert 2: 400 Errors
    alert2_json="{\"severity\":\"critical\",\"title\":\"400 Errors\",\"stream\":\"$P_STREAM\",\"alertType\":\"threshold\",\"aggregates\":{\"aggregateConfig\":[{\"aggregateFunction\":\"count\",\"conditions\":{\"operator\":null,\"conditionConfig\":[{\"column\":\"body\",\"operator\":\"contains\",\"value\":\"400\"}]},\"column\":\"body\",\"operator\":\">\",\"value\":10}]},\"evalConfig\":{\"rollingWindow\":{\"evalStart\":\"5h\",\"evalEnd\":\"now\",\"evalFrequency\":1}},\"targets\":[\"$target_id\"]}"
    
    response2=$(curl_with_retry "$P_URL/api/v1/alerts" "POST" "$alert2_json" "application/json" 3)
    if [[ $? -eq 0 ]]; then
        echo "Alert 2 (400 Errors) created successfully"
    else
        echo "Failed to create Alert 2 (400 Errors)"
        echo "Response: $response2"
    fi
    
    # Alert 3: Trace ID null
    alert3_json="{\"severity\":\"high\",\"title\":\"Trace ID null\",\"stream\":\"$P_STREAM\",\"alertType\":\"threshold\",\"aggregates\":{\"aggregateConfig\":[{\"aggregateFunction\":\"count\",\"conditions\":{\"operator\":null,\"conditionConfig\":[{\"column\":\"trace_id\",\"operator\":\"is null\",\"value\":null}]},\"column\":\"trace_id\",\"operator\":\">\",\"value\":0}]},\"evalConfig\":{\"rollingWindow\":{\"evalStart\":\"5h\",\"evalEnd\":\"now\",\"evalFrequency\":1}},\"targets\":[\"$target_id\"]}"
    response3=$(curl_with_retry "$P_URL/api/v1/alerts" "POST" "$alert3_json" "application/json" 3)
    if [[ $? -eq 0 ]]; then
        echo "Alert 3 (Trace ID null) created successfully"
    else
        echo "Failed to create Alert 3 (Trace ID null)"
        echo "Response: $response3"
    fi
}

# Main alerts function
run_alerts() {
    echo "Starting alert creation..."
    
    # Create target and get ID
    target_id=$(create_target)

    if [[ $? -eq 0 && -n "$target_id" ]]; then
        echo "Target creation successful, proceeding with alerts..."
        
        # Create alerts using the target ID
        create_alerts "$target_id"
        echo "Alert creation completed"
    else
        echo "Failed to create target, cannot proceed with alerts"
        return 1
    fi
}

# ==================== DASHBOARDS FUNCTIONALITY ====================

# Create dashboard
create_dashboard() {
    echo "Creating dashboard..." >&2
    
    response=$(curl -s -H "Content-Type: application/json" -H "$AUTH_HEADER" -X POST "$P_URL/api/v1/dashboards" -d @- << EOF
{
    "title": "Demo Dashboard",
    "tags": [
        "demo",
        "oss"
    ]
}
EOF
)
    
    curl_exit_code=$?
    
    if [[ $curl_exit_code -eq 0 && -n "$response" ]]; then
        # Extract dashboard ID from response
        dashboard_id=$(echo "$response" | jq -r '.dashboardId // empty')
        if [[ -n "$dashboard_id" ]]; then
            echo "Dashboard created successfully with ID: $dashboard_id" >&2
            echo "$dashboard_id"
            return 0
        else
            echo "Failed to extract dashboard ID from response" >&2
            echo "Response: $response" >&2
            return 1
        fi
    else
        echo "Failed to create dashboard" >&2
        echo "Curl exit code: $curl_exit_code" >&2
        echo "Response: $response" >&2
        return 1
    fi
}

# Update dashboard with tiles
update_dashboard() {
    local dashboard_id="$1"
    
    if [[ -z "$dashboard_id" ]]; then
        echo "Dashboard ID is required to update dashboard"
        return 1
    fi
    
    echo "Updating dashboard with ID: $dashboard_id"
    
    # Create the dashboard configuration with updated tiles
    dashboard_config=$(cat << EOF
{
    "title": "Demo Dashboard",
    "dashboardId": "$dashboard_id",
    "tags": [
        "demo",
        "oss"
    ],
    "isFavorite": true,
    "dashboardType": "Dashboard",
    "tiles": [
        {
            "tile_id": "01K017X5NG2SZ20PJ0EEYG9376",
            "title": "Service Error Rate Over Time",
            "chartQuery": {
                "x": {
                    "fields": [
                        {
                            "name": "p_timestamp",
                            "type": "time"
                        }
                    ],
                    "granularity": "minute"
                },
                "y": {
                    "fields": [
                        {
                            "name": "severity_number",
                            "aggregate": "COUNT"
                        }
                    ],
                    "groupBy": [
                        "severity_text"
                    ]
                },
                "filters": []
            },
            "dbName": "$P_STREAM",
            "chartType": "timeseries",
            "config": {
                "type": "bar",
                "colourScheme": "forest",
                "layout": {
                    "legendPosition": "bottom"
                },
                "axes": {
                    "x": {
                        "field": "time_bucket",
                        "title": "Time"
                    },
                    "y": {
                        "field": "COUNT_severity_number",
                        "title": "Event Count"
                    }
                },
                "advanced": {
                    "dataLabels": {
                        "enabled": false
                    },
                    "tooltip": {
                        "enabled": true,
                        "mode": "index",
                        "intersect": false
                    }
                }
            },
            "layout": {
                "w": 12,
                "h": 8,
                "x": 0,
                "y": 0,
                "i": "01K017X5NG2SZ20PJ0EEYG9376",
                "moved": false,
                "static": false
            }
        },
        {
            "tile_id": "01K027HTD413T9MP39KYEE42GS",
            "title": "Request Count by Service",
            "chartQuery": {
                "x": {
                    "fields": [
                        {
                            "name": "service.name",
                            "aggregate": "COUNT"
                        }
                    ],
                    "groupBy": [
                        "service.name"
                    ]
                },
                "y": {
                    "fields": [
                        {
                            "name": "url.path",
                            "aggregate": "COUNT"
                        }
                    ],
                    "groupBy": [
                        "url.path"
                    ]
                },
                "filters": []
            },
            "dbName": "$P_STREAM",
            "chartType": "line",
            "config": {
                "type": "line",
                "colourScheme": "cyber",
                "layout": {
                    "legendPosition": "bottom"
                },
                "axes": {
                    "x": {
                        "field": "service.name",
                        "title": "Service"
                    },
                    "y": {
                        "field": "COUNT_url.path",
                        "title": "Request Count"
                    }
                },
                "advanced": {
                    "dataLabels": {
                        "enabled": false
                    },
                    "tooltip": {
                        "enabled": true,
                        "mode": "index",
                        "intersect": false
                    }
                }
            },
            "layout": {
                "w": 4,
                "h": 8,
                "x": 0,
                "y": 8,
                "i": "01K027HTD413T9MP39KYEE42GS",
                "moved": false,
                "static": false
            }
        },
        {
            "tile_id": "01K027MQ5K75VSCFGVVN86MBMJ",
            "title": "Response Status Distribution by Upstream Cluster",
            "chartQuery": {
                "x": {
                    "fields": [
                        {
                            "name": "upstream.cluster",
                            "aggregate": "COUNT"
                        }
                    ],
                    "groupBy": [
                        "upstream.cluster"
                    ]
                },
                "y": {
                    "fields": [
                        {
                            "name": "severity_text",
                            "aggregate": "COUNT"
                        }
                    ],
                    "groupBy": [
                        "severity_text"
                    ]
                },
                "filters": []
            },
            "dbName": "$P_STREAM",
            "chartType": "bar",
            "config": {
                "type": "bar",
                "colourScheme": "dusk",
                "layout": {
                    "legendPosition": "bottom"
                },
                "axes": {
                    "x": {
                        "field": "upstream.cluster",
                        "title": "Upstream Cluster"
                    },
                    "y": {
                        "field": "COUNT_severity_text",
                        "title": "Response Count"
                    }
                },
                "advanced": {
                    "dataLabels": {
                        "enabled": false
                    },
                    "tooltip": {
                        "enabled": true,
                        "mode": "index",
                        "intersect": false
                    }
                }
            },
            "layout": {
                "w": 8,
                "h": 8,
                "x": 4,
                "y": 8,
                "i": "01K027MQ5K75VSCFGVVN86MBMJ",
                "moved": false,
                "static": false
            }
        },
        {
            "tile_id": "01K027RM6R3EQ6K960ECSKP5PX",
            "title": "User Agent Distribution by Source Address",
            "chartQuery": {
                "x": {
                    "fields": [
                        {
                            "name": "source.address",
                            "aggregate": "COUNT"
                        }
                    ],
                    "groupBy": [
                        "source.address"
                    ]
                },
                "y": {
                    "fields": [
                        {
                            "name": "user_agent.original",
                            "aggregate": "COUNT"
                        }
                    ],
                    "groupBy": [
                        "user_agent.original"
                    ]
                },
                "filters": []
            },
            "dbName": "$P_STREAM",
            "chartType": "area",
            "config": {
                "type": "area",
                "colourScheme": "forest",
                "layout": {
                    "legendPosition": "bottom"
                },
                "axes": {
                    "x": {
                        "field": "source.address",
                        "title": "Source IP Address"
                    },
                    "y": {
                        "field": "COUNT_user_agent.original",
                        "title": "User Agent Count"
                    }
                },
                "advanced": {
                    "dataLabels": {
                        "enabled": false
                    },
                    "tooltip": {
                        "enabled": true,
                        "mode": "index",
                        "intersect": false
                    }
                }
            },
            "layout": {
                "w": 7,
                "h": 7,
                "x": 0,
                "y": 16,
                "i": "01K027RM6R3EQ6K960ECSKP5PX",
                "moved": false,
                "static": false
            }
        }
    ]
}
EOF
)
    
    response=$(curl_with_retry "$P_URL/api/v1/dashboards/$dashboard_id" "PUT" "$dashboard_config" "application/json" 3)
    if [[ $? -eq 0 ]]; then
        echo "Dashboard updated successfully"
        return 0
    else
        echo "Failed to update dashboard"
        echo "Response: $response"
        return 1
    fi
}

# Main dashboards function
run_dashboards() {
    echo "Starting dashboard creation..."
    
    # Create dashboard and get ID
    dashboard_id=$(create_dashboard)

    if [[ $? -eq 0 && -n "$dashboard_id" ]]; then
        echo "Dashboard creation successful, proceeding with tiles..."
        
        # Update dashboard with tiles
        update_dashboard "$dashboard_id"
        echo "Dashboard creation completed"
    else
        echo "Failed to create dashboard, cannot proceed with tiles"
        return 1
    fi
}

# ==================== MAIN EXECUTION ====================

# Display usage
show_usage() {
    echo "Usage: $0 [ACTION=ingest|filters|alerts|dashboards|all]"
    echo ""
    echo "Environment variables:"
    echo "  P_URL       - API URL (default: http://localhost:8000)"
    echo "  P_USERNAME  - Username (default: admin)"
    echo "  P_PASSWORD  - Password (default: admin)"
    echo "  P_STREAM    - Stream name (default: demodata)"
    echo "  ACTION      - Action to perform (default: ingest)"
    echo ""
    echo "Actions:"
    echo "  ingest      - Ingest demo log data"
    echo "  filters     - Create SQL and saved filters"
    echo "  alerts      - Create alerts and webhook targets"
    echo "  dashboards  - Create demo dashboard with tiles"
    echo "  all         - Run all actions in sequence"
    echo ""
    echo "Examples:"
    echo "  ACTION=ingest ./script.sh"
    echo "  ACTION=filters P_STREAM=mystream ./script.sh"
    echo "  ACTION=dashboards ./script.sh"
    echo "  ACTION=all ./script.sh"
}

# Main execution logic
main() {
    case "$ACTION" in
        "ingest")
            run_ingest
            ;;
        "filters")
            run_filters
            ;;
        "alerts")
            run_alerts
            ;;
        "dashboards")
            run_dashboards
            ;;
        "all")
            echo "Running all actions..."
            run_ingest
            echo "Waiting before creating filters..."
            run_filters
            echo "Waiting before creating alerts..."
            run_alerts
            echo "Waiting before creating dashboards..."
            run_dashboards
            echo "All actions completed"
            ;;
        "help"|"--help"|"-h")
            show_usage
            exit 0
            ;;
        *)
            echo "Unknown action: $ACTION"
            show_usage
            exit 1
            ;;
    esac
}

# Execute main function
main "$@"
exit $?